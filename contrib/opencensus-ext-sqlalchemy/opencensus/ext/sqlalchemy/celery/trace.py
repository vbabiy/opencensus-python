# Copyright 2017, OpenCensus Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copyright 2017, OpenCensus Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

import celery
import wrapt
from celery.signals import before_task_publish, after_task_publish, task_prerun, task_failure, task_success

from opencensus.trace import attributes_helper
from opencensus.trace import execution_context
from opencensus.trace import span as span_module

log = logging.getLogger(__name__)

MODULE_NAME = 'celery'


def trace_integration(tracer=None):
    """Wrap the celery library to trace it."""
    log.info('Integrated module: {}'.format(MODULE_NAME))

    execution_context.set_opencensus_tracer(tracer)


@before_task_publish.connect
def create_publish_span(**kwargs):
    print(**kwargs)
    _tracer = execution_context.get_opencensus_tracer()
    _span = _tracer.start_span()
    _span.name = '[celery]publish'
    _span.span_kind = span_module.SpanKind.CLIENT


@after_task_publish.connect
def end_publish_span(**kwargs):
    print(**kwargs)
    _tracer = execution_context.get_opencensus_tracer()
    _tracer.end_span()


@task_prerun.connect
def create_publish_span(**kwargs):
    print(**kwargs)
    _tracer = execution_context.get_opencensus_tracer()
    _span = _tracer.start_span()
    _span.name = '[celery]run_task'
    _span.span_kind = span_module.SpanKind.CLIENT


@task_success.connect
def end_successful_task_span(**kwargs):
    print(**kwargs)
    _tracer = execution_context.get_opencensus_tracer()
    _tracer.end_span()


@task_failure.connect
def end_failed_task_span(traceback=None, **kwargs):
    print(**kwargs)
    _tracer = execution_context.get_opencensus_tracer()
    _tracer.add_attribute_to_current_span(
        attributes_helper['COMMON_ATTRIBUTES']['STACKTRACE'], str(traceback))
    _tracer.end_span()
