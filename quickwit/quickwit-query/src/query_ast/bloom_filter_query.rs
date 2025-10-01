use crate::query_ast::tantivy_query_ast::TantivyQueryAst;
use crate::query_ast::{AutomatonQuery, BuildTantivyAst, JsonPathPrefix, QueryAst, TermSetQuery};
use crate::tokenizers::TokenizerManager;
use crate::{InvalidQuery, find_field_or_hit_dynamic};
use anyhow::Context;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};
use tantivy::Term;
use tantivy::schema::{Field, FieldType, Schema as TantivySchema};

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct BloomFilterQuery {
    pub field: String,
    pub bloom_filter_base64: String,
    /// Support missing fields
    pub lenient: bool,
}

impl BloomFilterQuery {
    pub fn to_field_and_bloom_filter(
        &self,
        schema: &TantivySchema,
    ) -> Result<(Field, Option<Vec<u8>>, String), InvalidQuery> {
        let Some((field, field_entry, json_path)) = find_field_or_hit_dynamic(&self.field, schema)
        else {
            return Err(InvalidQuery::FieldDoesNotExist {
                full_path: self.field.clone(),
            });
        };
        let field_type = field_entry.field_type();

        match field_type {
            FieldType::Str(text_options) => {
                text_options.get_indexing_options().ok_or_else(|| {
                    InvalidQuery::SchemaError(format!(
                        "field {} is not full-text searchable",
                        field_entry.name()
                    ))
                })?;

                Ok((field, None, self.bloom_filter_base64.clone()))
            }
            FieldType::JsonObject(json_options) => {
                json_options.get_text_indexing_options().ok_or_else(|| {
                    InvalidQuery::SchemaError(format!(
                        "field {} is not full-text searchable",
                        field_entry.name()
                    ))
                })?;

                let mut term_for_path = Term::from_field_json_path(
                    field,
                    json_path,
                    json_options.is_expand_dots_enabled(),
                );
                term_for_path.append_type_and_str("");

                let value = term_for_path.value();
                // We skip the 1st byte which is a marker to tell this is json. This isn't present
                // in the dictionary
                let byte_path_prefix = value.as_serialized()[1..].to_owned();


               // println!("{}", String::from_utf8(byte_path_prefix.clone()).unwrap());

                Ok((
                    field,
                    Some(byte_path_prefix),
                    self.bloom_filter_base64.clone(),
                ))
            }
            _ => Err(InvalidQuery::SchemaError(
                "trying to run a bloomfilter query on a non-text field".to_string(),
            )),
        }
    }
}

impl BuildTantivyAst for BloomFilterQuery {
    fn build_tantivy_ast_impl(
        &self,
        schema: &TantivySchema,
        tokenizer_manager: &TokenizerManager,
        search_fields: &[String],
        with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let (field, _json_path_bytes, bloom_filter_base64) = self.to_field_and_bloom_filter(schema)?;
        if let Ok(bloom_filter) = BASE64.decode(bloom_filter_base64) {
            return Ok(TantivyQueryAst::from(
                tantivy::query::BloomfilterQuery::new(
                    Term::from_field_bytes(field, &bloom_filter),
                    self.field.clone(),
                ),
            ));
        }
        Err(InvalidQuery::Other(anyhow::Error::msg(
            "invalid bloom_filter_base64",
        )))
    }
}

impl From<BloomFilterQuery> for QueryAst {
    fn from(bloom_filter_query: BloomFilterQuery) -> Self {
        QueryAst::BloomFilter(bloom_filter_query)
    }
}

#[cfg(test)]
mod tests {
    use tantivy::schema::{FAST, TEXT};

    use crate::query_ast::bloom_filter_query::BloomFilterQuery;
    use tantivy::schema::{Field, FieldType, Schema as TantivySchema};

    #[test]
    fn test_bloomfilter_query_text_field() {
        let mut schema_builder = TantivySchema::builder();
        schema_builder.add_text_field("field", TEXT | FAST);
        let schema = schema_builder.build();

        let bloom_filter_base64 = "AbQAAAAAAAAACgAAAGY+SCeG/ZunYn1NCgb7ZNqDjdukxYCCqonsR0YMd80ICAABAAAAAQAAQAAABAAAAAAAAAAAAAAQAAAAAAAAAAABAAAEAIAAAAAAAAEAAAAAAAAAAAAAAAgAAAAAAAAAIAAAAAAQAAAAAAAAAAAAAAACAAAAAAAABAAAAAAAAACEAAAAAAAAAABAAAAAAAAAAAIAAAAAAAAKAAgAAAAAAAAAAgCAAAAAACAAAAAACABAAAAAAAAAAAAAACIAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAA".to_string();

        let query = BloomFilterQuery {
            field: "field".to_string(),
            bloom_filter_base64: bloom_filter_base64.clone(),
            lenient: false,
        };

        let (field, path, bloom_filter_base64) = query.to_field_and_bloom_filter(&schema).unwrap();
        assert_eq!(field, schema.get_field("field").unwrap());
        assert!(path.is_none());
        assert_eq!(bloom_filter_base64, query.bloom_filter_base64);
    }
}
