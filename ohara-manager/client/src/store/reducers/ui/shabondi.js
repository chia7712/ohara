/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as actions from 'store/actions';

const initialState = {
  loading: false,
  lastUpdated: null,
  error: null,
};

export default function reducer(state = initialState, action) {
  switch (action.type) {
    // Ensure the loading flag is set to true when both REQUEST and TRIGGER
    // actions are fired. This prevents the fetch action from making multiple
    // requests at the same time
    case actions.fetchShabondis.REQUEST:
    case actions.fetchConnectors.TRIGGER:
      return {
        ...state,
        loading: true,
        error: null,
      };

    case actions.fetchShabondis.SUCCESS:
      return {
        ...state,
        loading: false,
        lastUpdated: new Date(),
      };
    case actions.fetchShabondis.FAILURE:
      return {
        ...state,
        loading: false,
        error: action.payload,
      };
    default:
      return state;
  }
}
