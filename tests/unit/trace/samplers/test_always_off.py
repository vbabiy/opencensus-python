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

import unittest

import mock


class TestAlwaysOffSampler(unittest.TestCase):
    def test_should_sample(self):
        from opencensus.trace import samplers

        sampler = samplers.AlwaysOffSampler()
        mock_context = mock.Mock()
        mock_context.trace_id = 'fake_id'
        should_sample = sampler.should_sample(mock_context)

        self.assertFalse(should_sample)
