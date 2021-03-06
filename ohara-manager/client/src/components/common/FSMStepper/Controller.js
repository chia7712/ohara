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

import PropTypes from 'prop-types';
import { toUpper } from 'lodash';

import Grid from '@material-ui/core/Grid';
import Button from '@material-ui/core/Button';

import ArrowBackIcon from '@material-ui/icons/ArrowBack';
import RefreshIcon from '@material-ui/icons/Refresh';

const Controller = (props) => {
  const { revertible, revertText: textOfRevertButton, state, send } = props;

  const isIdle = state.matches('idle');
  const isForward = !!state?.context?.forward;
  const hasError = !!state?.context?.error;

  const showRetryButton = isIdle && hasError;
  const showRevertButton = isIdle && hasError && isForward;

  return (
    <>
      <Grid
        alignItems="center"
        container
        direction="row"
        justify="center"
        spacing={2}
      >
        {revertible && (
          <Grid item>
            <Button
              color="primary"
              data-testid="stepper-revert-button"
              disabled={!showRevertButton}
              onClick={() => send('REVERT')}
              startIcon={<ArrowBackIcon />}
            >
              {toUpper(textOfRevertButton)}
            </Button>
          </Grid>
        )}

        <Grid item>
          <Button
            color="primary"
            data-testid="stepper-retry-button"
            disabled={!showRetryButton}
            onClick={() => send('RETRY')}
            startIcon={<RefreshIcon />}
          >
            RETRY
          </Button>
        </Grid>
      </Grid>
    </>
  );
};

Controller.propTypes = {
  state: PropTypes.shape({
    context: PropTypes.shape({
      activeStep: PropTypes.number,
      steps: PropTypes.array,
      forward: PropTypes.bool,
      error: PropTypes.object,
    }),
    matches: PropTypes.func,
  }).isRequired,
  revertible: PropTypes.bool,
  revertText: PropTypes.string,
  send: PropTypes.func.isRequired,
};

Controller.defaultProps = {
  revertible: false,
  revertText: 'Rollback',
};

export default Controller;
