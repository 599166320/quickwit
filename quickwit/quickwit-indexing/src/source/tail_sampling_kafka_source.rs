// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use itertools::Itertools;
use oneshot;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::KafkaSourceParams;
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint};
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::{IndexUid, Position};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, DefaultConsumerContext, Rebalance,
};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{ClientContext, Message, Offset, TopicPartitionList};
use serde_json::{Value as JsonValue, json};
use tail_sampling::bloom::RotatingBloom;
use tail_sampling::conf::CONFIG;
use tail_sampling::kafka::group_consumer::{
    ErrorLogProcessor, ErrorTraceIdProcessor, GroupConsumerProcessor,
};
use tail_sampling::kafka::{self, KafkaMessage};
use tail_sampling::log_config;
use tail_sampling::metric::jemallocator_metrics::update_jemalloc_metrics;
use tail_sampling::metric::{self, HttpServerContext, IP_AND_PID, PushGateway};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, spawn_blocking};
use tokio::time;
use tracing::{Instrument, debug, info, warn};
use tokio::sync::broadcast;

use crate::actors::DocProcessor;
use crate::models::{NewPublishLock, PublishLock};
use crate::source::{
    BATCH_NUM_BYTES_LIMIT, BatchBuilder, EMIT_BATCHES_TIMEOUT, Source, SourceContext,
    SourceRuntime, TypedSourceFactory,
};
pub struct TailSamplingKafkaSourceFactory;

#[async_trait]
impl TypedSourceFactory for TailSamplingKafkaSourceFactory {
    type Source = TailSamplingKafkaSource;
    type Params = KafkaSourceParams;

    async fn typed_create_source(
        source_runtime: SourceRuntime,
        params: KafkaSourceParams,
    ) -> anyhow::Result<Self::Source> {
        TailSamplingKafkaSource::try_new(source_runtime, params).await
    }
}

#[derive(Default)]
pub struct TailSamplingKafkaSourceState {
    /// Partitions IDs assigned to the source.
    pub assigned_partitions: HashMap<i32, PartitionId>,
    /// Offset for each partition of the last message received.
    pub current_positions: HashMap<i32, Position>,
    /// Number of inactive partitions, i.e., that have reached EOF.
    pub num_inactive_partitions: usize,
    /// Number of bytes processed by the source.
    pub num_bytes_processed: u64,
    /// Number of messages processed by the source (including invalid messages).
    pub num_messages_processed: u64,
    // Number of invalid messages, i.e., that were empty or could not be parsed.
    pub num_invalid_messages: u64,
    /// Number of rebalances the consumer went through.
    pub num_rebalances: usize,
}

pub struct TailSamplingKafkaSource {
    source_runtime: SourceRuntime,
    poll_loop_jhs: Vec<JoinHandle<()>>,
    events_rx: mpsc::Receiver<Vec<KafkaMessage>>,
    publish_lock: PublishLock,
    state: TailSamplingKafkaSourceState,
    pub partition_bloomfilter_map: Arc<DashMap<i32, Arc<RwLock<RotatingBloom>>>>,
    shutdown_tx: broadcast::Sender<()>,
}

impl fmt::Debug for TailSamplingKafkaSource {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("TailSamplingKafkaSource")
            .field("index_uid", self.source_runtime.index_uid())
            .field("source_id", &self.source_runtime.source_id())
            .finish()
    }
}

impl TailSamplingKafkaSource {
    pub async fn try_new(
        source_runtime: SourceRuntime,
        source_params: KafkaSourceParams,
    ) -> anyhow::Result<Self> {
        let mut poll_loop_jhs = Vec::new();

        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let mut shutdown_rx = shutdown_tx.subscribe();
        if CONFIG.task_switch.error_log_consumer_task_enable {
            let poll_loop_jh = tokio::spawn(
                async move {
                    //错误日志解析和处理,错误traceID写写入otel topic,并且还需要同步到另外一朵云
                    let mut error_log_processor = ErrorLogProcessor::init(
                        &CONFIG.error_log_consumer.consumer_common_properties,
                        shutdown_rx
                    );
                    error_log_processor.producer_properties =
                        Some(CONFIG.error_log_consumer.error_log_produce.clone());
                    error_log_processor.error_trace_id_for_other_cloud_producer =
                        Some(CONFIG.error_trace_id_for_other_cloud_producer.clone());
                    error_log_processor.start().await;
                }
                .instrument(tracing::info_span!("error_log_consumer_task")),
            );
            poll_loop_jhs.push(poll_loop_jh);
        }

        shutdown_rx = shutdown_tx.subscribe();
        if CONFIG.task_switch.error_trace_id_task_enable {
            let poll_loop_jh = tokio::spawn(
                async move {
                    //消费另一朵云的traceId,写入到otel topic，方便更新bloom filter
                    let mut error_trace_id_processor: ErrorTraceIdProcessor =
                        ErrorTraceIdProcessor::init(
                            &CONFIG.error_trace_id_consumer.consumer_common_properties,
                            shutdown_rx
                        );
                    error_trace_id_processor.producer_properties = Some(
                        CONFIG
                            .error_trace_id_consumer
                            .error_trace_id_produce
                            .clone(),
                    );
                    error_trace_id_processor.start().await;
                }
                .instrument(tracing::info_span!("error_trace_id_task")),
            );
            poll_loop_jhs.push(poll_loop_jh);
        }


        let shutdown_tx_clone = shutdown_tx.clone();
        let (partition_bloomfilter_map_tx, mut partition_bloomfilter_map_rx) = mpsc::channel(1);
        let (events_tx, mut events_rx) = mpsc::channel(CONFIG.discard_queue_size.expect("The quickwit queue size must be greater than 0"));
        let poll_loop_jh = tokio::spawn(
            async move {
                let mut kafka_consumer = kafka::KafkaConsumer::init(shutdown_tx_clone).await;
                kafka_consumer.discard_spans_sender = Some(events_tx);
                partition_bloomfilter_map_tx
                    .send(kafka_consumer.partition_bloomfilter_map.clone())
                    .await
                    .expect("could not send partition_bloomfilter_map");
                kafka_consumer.start().await;
            }
            .instrument(tracing::info_span!("rt_dl_task")),
        );
        poll_loop_jhs.push(poll_loop_jh);

        if CONFIG.metric.pushgateway_enable {
            let pushgateway = PushGateway {
                interval_sec: CONFIG.metric.interval_sec,
                push_url: format!(
                    "{}/{}",
                    CONFIG.metric.pushgateway_url,
                    IP_AND_PID.to_string()
                ),
            };
            pushgateway.start().await;
        }

        let poll_loop_jh = tokio::spawn(async {
            loop {
                update_jemalloc_metrics();
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });
        poll_loop_jhs.push(poll_loop_jh);

        let partition_bloomfilter_map = partition_bloomfilter_map_rx
            .recv()
            .await
            .expect("Failed to receiver bloom filer.");
        let context = Arc::new(HttpServerContext {
            partition_bloomfilter_map: partition_bloomfilter_map.clone(),
        });

        let poll_loop_jh = tokio::spawn(async move {
            metric::start_metrics_server(context).await;
        });
        poll_loop_jhs.push(poll_loop_jh);
        let publish_lock = PublishLock::default();
        Ok(TailSamplingKafkaSource {
            source_runtime,
            poll_loop_jhs,
            events_rx,
            publish_lock,
            state: TailSamplingKafkaSourceState::default(),
            partition_bloomfilter_map,
            shutdown_tx
        })
    }

    async fn process_message(
        &mut self,
        messages: Vec<KafkaMessage>,
        batch: &mut BatchBuilder,
    ) -> anyhow::Result<()> {
        for message in messages {
            batch.add_owned_messages(message.owned_message);
        }
        Ok(())
    }

    fn truncate(&self, checkpoint: SourceCheckpoint) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Source for TailSamplingKafkaSource {
    async fn initialize(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        info!(
            index_uid=%self.source_runtime.index_uid(),
            source_id=%self.source_runtime.source_id(),
            "Initialized TailSamplingKafkaSource"
        );
        let publish_lock = self.publish_lock.clone();
        ctx.send_message(doc_processor_mailbox, NewPublishLock(publish_lock))
            .await?;
        Ok(())
    }

    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let now = Instant::now();
        let mut batch_builder = BatchBuilder::new(SourceType::Kafka);
        let deadline = time::sleep(*EMIT_BATCHES_TIMEOUT);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                event_opt = self.events_rx.recv() => {
                    let events = event_opt.ok_or_else(|| ActorExitStatus::from(anyhow!("consumer was dropped")))?;
                    self.process_message(events, &mut batch_builder).await?;
                    if batch_builder.num_bytes >= BATCH_NUM_BYTES_LIMIT {
                        break;
                    }
                }
                _ = &mut deadline => {
                    break;
                }
            }
            ctx.record_progress();
        }

        if batch_builder.num_bytes > 0 {
            debug!(
                num_docs=%batch_builder.docs.len(),
                num_bytes=%batch_builder.num_bytes,
                num_millis=%now.elapsed().as_millis(),
                "sending doc batch to indexer"
            );
            let message = batch_builder.build();
            ctx.send_message(doc_processor_mailbox, message).await?;
        }
        Ok(Duration::default())
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        info!(
            index_uid=%self.source_runtime.index_uid(),
            source_id=%self.source_runtime.source_id(),
            "suggest_truncate..."
        );
        self.truncate(checkpoint)?;
        Ok(())
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {

        let _ = self.shutdown_tx.send(());

        for poll_loop_jhself in &self.poll_loop_jhs {
            poll_loop_jhself.abort();
        }
        info!(
            index_uid=%self.source_runtime.index_uid(),
            source_id=%self.source_runtime.source_id(),
            "Finalized TailSamplingKafkaSource"
        );
        Ok(())
    }

    fn name(&self) -> String {
        format!("{self:?}")
    }

    fn observable_state(&self) -> JsonValue {
        info!(
            index_uid=%self.source_runtime.index_uid(),
            source_id=%self.source_runtime.source_id(),
            "observable_state..."
        );
        let assigned_partitions: Vec<&i32> =
            self.state.assigned_partitions.keys().sorted().collect();
        let current_positions: Vec<(&i32, &Position)> =
            self.state.current_positions.iter().sorted().collect();
        json!({
            "index_id": self.source_runtime.index_id(),
            "source_id": self.source_runtime.source_id(),
            "num_inactive_partitions": self.state.num_inactive_partitions,
            "num_bytes_processed": self.state.num_bytes_processed,
            "num_messages_processed": self.state.num_messages_processed,
            "num_invalid_messages": self.state.num_invalid_messages,
            "num_rebalances": self.state.num_rebalances,
        })
    }
}

pub(super) async fn check_connectivity(params: KafkaSourceParams) -> anyhow::Result<()> {
    let params_info = format!("{params:?}");
    info!(
        params=%params_info,
        "TailSamplingKafkaSource check_connectivity"
    );
    Ok(())
}
