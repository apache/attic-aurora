/**
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
package org.apache.aurora.scheduler.cron.quartz;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import com.twitter.common.base.Supplier;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffHelper;

import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import static org.apache.aurora.gen.ScheduleStatus.KILLING;

/**
 * Encapsulates the logic behind a single trigger of a single job key. Multiple executions may run
 * concurrently but only a single instance will be active at a time per job key.
 *
 * <p>
 * Executions may block for long periods of time when waiting for a kill to complete. The Quartz
 * scheduler should therefore be configured with a large number of threads.
 */
@DisallowConcurrentExecution
class AuroraCronJob implements Job {
  private static final Logger LOG = Logger.getLogger(AuroraCronJob.class.getName());

  private static final AtomicLong CRON_JOB_TRIGGERS = Stats.exportLong("cron_job_triggers");
  private static final AtomicLong CRON_JOB_MISFIRES = Stats.exportLong("cron_job_misfires");
  private static final AtomicLong CRON_JOB_PARSE_FAILURES =
      Stats.exportLong("cron_job_parse_failures");
  private static final AtomicLong CRON_JOB_COLLISIONS = Stats.exportLong("cron_job_collisions");

  @VisibleForTesting
  static final Optional<String> KILL_AUDIT_MESSAGE = Optional.of("Killed by cronScheduler");

  private final Storage storage;
  private final StateManager stateManager;
  private final CronJobManager cronJobManager;
  private final BackoffHelper delayedStartBackoff;

  @Inject
  AuroraCronJob(
      Config config,
      Storage storage,
      StateManager stateManager,
      CronJobManager cronJobManager) {

    this.storage = checkNotNull(storage);
    this.stateManager = checkNotNull(stateManager);
    this.cronJobManager = checkNotNull(cronJobManager);
    this.delayedStartBackoff = checkNotNull(config.getDelayedStartBackoff());
  }

  private static final class DeferredLaunch {
    private final Map<Integer, ITaskConfig> pendingTasks;
    private final Set<String> activeTaskIds;

    private DeferredLaunch(Map<Integer, ITaskConfig> pendingTasks, Set<String> activeTaskIds) {
      this.pendingTasks = pendingTasks;
      this.activeTaskIds = activeTaskIds;
    }
  }

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    // We assume quartz prevents concurrent runs of this job for a given job key. This allows us
    // to avoid races where we might kill another run's tasks.
    checkState(context.getJobDetail().isConcurrentExectionDisallowed());

    doExecute(Quartz.auroraJobKey(context.getJobDetail().getKey()));
  }

  @VisibleForTesting
  void doExecute(final IJobKey key) throws JobExecutionException {
    final String path = JobKeys.canonicalString(key);

    final Optional<DeferredLaunch> deferredLaunch = storage.write(
        new Storage.MutateWork.Quiet<Optional<DeferredLaunch>>() {
          @Override
          public Optional<DeferredLaunch> apply(Storage.MutableStoreProvider storeProvider) {
            Optional<IJobConfiguration> config =
                storeProvider.getJobStore().fetchJob(cronJobManager.getManagerKey(), key);
            if (!config.isPresent()) {
              LOG.warning(String.format(
                  "Cron was triggered for %s but no job with that key was found in storage.",
                  path));
              CRON_JOB_MISFIRES.incrementAndGet();
              return Optional.absent();
            }

            SanitizedCronJob cronJob;
            try {
              cronJob = SanitizedCronJob.fromUnsanitized(config.get());
            } catch (ConfigurationManager.TaskDescriptionException | CronException e) {
              LOG.warning(String.format(
                  "Invalid cron job for %s in storage - failed to parse with %s", key, e));
              CRON_JOB_PARSE_FAILURES.incrementAndGet();
              return Optional.absent();
            }

            CronCollisionPolicy collisionPolicy = cronJob.getCronCollisionPolicy();
            LOG.info(String.format(
                "Cron triggered for %s at %s with policy %s", path, new Date(), collisionPolicy));
            CRON_JOB_TRIGGERS.incrementAndGet();

            ImmutableMap<Integer, ITaskConfig> pendingTasks =
                ImmutableMap.copyOf(cronJob.getSanitizedConfig().getTaskConfigs());

            final Query.Builder activeQuery = Query.jobScoped(key).active();
            Set<String> activeTasks =
                Tasks.ids(storeProvider.getTaskStore().fetchTasks(activeQuery));

            if (activeTasks.isEmpty()) {
              stateManager.insertPendingTasks(pendingTasks);
              return Optional.absent();
            }

            CRON_JOB_COLLISIONS.incrementAndGet();
            switch (collisionPolicy) {
              case KILL_EXISTING:
                return Optional.of(new DeferredLaunch(pendingTasks, activeTasks));

              case RUN_OVERLAP:
                LOG.severe(String.format("Ignoring trigger for job %s with deprecated collision"
                    + "policy RUN_OVERLAP due to unterminated active tasks.", path));
                return Optional.absent();

              case CANCEL_NEW:
                return Optional.absent();

              default:
                LOG.severe("Unrecognized cron collision policy: " + collisionPolicy);
                return Optional.absent();
            }
          }
        }
    );

    if (!deferredLaunch.isPresent()) {
      return;
    }

    for (String taskId : deferredLaunch.get().activeTaskIds) {
      stateManager.changeState(
          taskId,
          Optional.<ScheduleStatus>absent(),
          KILLING,
          KILL_AUDIT_MESSAGE);
    }
    LOG.info(String.format("Waiting for job to terminate before launching cron job %s.", path));

    final Query.Builder query = Query.taskScoped(deferredLaunch.get().activeTaskIds).active();
    try {
      // NOTE: We block the quartz execution thread here until we've successfully killed our
      // ancestor. We mitigate this by using a cached thread pool for quartz.
      delayedStartBackoff.doUntilSuccess(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          if (Storage.Util.consistentFetchTasks(storage, query).isEmpty()) {
            LOG.info("Initiating delayed launch of cron " + path);
            stateManager.insertPendingTasks(deferredLaunch.get().pendingTasks);
            return true;
          } else {
            LOG.info("Not yet safe to run cron " + path);
            return false;
          }
        }
      });
    } catch (InterruptedException e) {
      LOG.log(Level.WARNING, "Interrupted while trying to launch cron " + path, e);
      Thread.currentThread().interrupt();
      throw new JobExecutionException(e);
    }
  }

  static class Config {
    private final BackoffHelper delayedStartBackoff;

    Config(BackoffHelper delayedStartBackoff) {
      this.delayedStartBackoff = checkNotNull(delayedStartBackoff);
    }

    public BackoffHelper getDelayedStartBackoff() {
      return delayedStartBackoff;
    }
  }
}
