#!/bin/bash
set -e

# 安装依赖
echo "安装必要的依赖..."
if ! command -v cross &> /dev/null; then
    cargo install cross --force
fi

# 添加目标平台
echo "添加x86_64-unknown-linux-gnu目标..."
rustup target add x86_64-unknown-linux-gnu

# 强制安装工具链（如果需要）
echo "强制安装工具链..."
rustup toolchain add 1.88-x86_64-unknown-linux-gnu --profile minimal --force-non-host || true

# 设置环境变量
export QW_COMMIT_DATE=$(TZ=UTC0 git log -1 --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ)
export QW_COMMIT_HASH=$(git rev-parse HEAD)
export QW_COMMIT_TAGS=$(git tag --points-at HEAD | tr '\n' ',')

# 构建UI
echo "构建React UI..."
make build-ui

# 交叉编译
echo "开始交叉编译 x86_64-unknown-linux-gnu..."
export CROSS_CONTAINER_OPTS="-v $(pwd)/tail-sampling:/Users/hk00518ml/rust-project/tail-sampling:rw"
#export ZSTD_SYS_USE_PKG_CONFIG=1
#export PKG_CONFIG_ALLOW_CROSS=1
cd quickwit
cross build --release --features release-feature-vendored-set --target x86_64-unknown-linux-gnu --bin quickwit

echo "编译完成！"