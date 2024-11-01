use std::sync::Arc;

use async_trait::async_trait;
use common_runtime::RuntimeRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::streaming_sink::{
    StreamingSink, StreamingSinkExecuteOutput, StreamingSinkFinalizeOutput,
    StreamingSinkOutputType, StreamingSinkState,
};
use crate::{
    dispatcher::{Dispatcher, UnorderedDispatcher},
    ExecutionRuntimeHandle, MaybeFuture,
};

struct LimitSinkState {
    remaining: usize,
}

impl LimitSinkState {
    fn new(remaining: usize) -> Self {
        Self { remaining }
    }

    fn get_remaining_mut(&mut self) -> &mut usize {
        &mut self.remaining
    }
}

impl StreamingSinkState for LimitSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct LimitSink {
    limit: usize,
}

impl LimitSink {
    pub fn new(limit: usize) -> Self {
        Self { limit }
    }
}

#[async_trait]
impl StreamingSink for LimitSink {
    #[instrument(skip_all, name = "LimitSink::sink")]
    fn execute(
        &self,
        input: &Arc<MicroPartition>,
        mut state: Box<dyn StreamingSinkState>,
        runtime_ref: &RuntimeRef,
    ) -> StreamingSinkExecuteOutput {
        let input_num_rows = input.len();

        let remaining = state
            .as_any_mut()
            .downcast_mut::<LimitSinkState>()
            .expect("Limit sink should have LimitSinkState")
            .get_remaining_mut();
        use std::cmp::Ordering::{Equal, Greater, Less};
        match input_num_rows.cmp(remaining) {
            Less => {
                *remaining -= input_num_rows;
                MaybeFuture::Immediate(Ok((state, StreamingSinkOutputType::NeedMoreInput(None))))
            }
            Equal => {
                *remaining = 0;
                MaybeFuture::Immediate(Ok((state, StreamingSinkOutputType::Finished(None))))
            }
            Greater => {
                let input = input.clone();
                let to_head = *remaining;
                *remaining = 0;
                let fut = runtime_ref.spawn(async move {
                    let taken = input.head(to_head)?;
                    Ok((state, StreamingSinkOutputType::Finished(Some(taken.into()))))
                });

                MaybeFuture::Future(fut)
            }
        }
    }

    fn name(&self) -> &'static str {
        "Limit"
    }

    fn finalize(
        &self,
        _states: Vec<Box<dyn StreamingSinkState>>,
        _runtime_ref: &RuntimeRef,
    ) -> StreamingSinkFinalizeOutput {
        MaybeFuture::Immediate(Ok(None))
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(LimitSinkState::new(self.limit))
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn make_dispatcher(
        &self,
        _runtime_handle: &ExecutionRuntimeHandle,
        _maintain_order: bool,
    ) -> Arc<dyn Dispatcher> {
        // LimitSink should be greedy, and accept all input as soon as possible.
        // It is also not concurrent, so we don't need to worry about ordering.
        Arc::new(UnorderedDispatcher::new(None))
    }
}
