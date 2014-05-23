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
package org.apache.aurora.scheduler.base;

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Utility class providing convenience functions relating to JobKeys.
 */
public final class JobKeys {
  private JobKeys() {
    // Utility class.
  }

  public static final Function<IJobConfiguration, IJobKey> FROM_CONFIG =
      new Function<IJobConfiguration, IJobKey>() {
        @Override
        public IJobKey apply(IJobConfiguration job) {
          return job.getKey();
        }
      };

  public static final Function<IJobKey, String> TO_ROLE =
      new Function<IJobKey, String>() {
        @Override
        public String apply(IJobKey jobKey) {
          return jobKey.getRole();
        }
      };

  public static final Function<IJobKey, String> TO_ENVIRONMENT =
      new Function<IJobKey, String>() {
        @Override
        public String apply(IJobKey jobKey) {
          return jobKey.getEnvironment();
        }
      };

  public static final Function<IJobKey, String> TO_JOB_NAME =
      new Function<IJobKey, String>() {
        @Override
        public String apply(IJobKey jobKey) {
          return jobKey.getName();
        }
      };

  public static final Function<IJobConfiguration, String> CONFIG_TO_ROLE =
      Functions.compose(TO_ROLE, FROM_CONFIG);

  /**
   * Check that a jobKey struct is valid.
   *
   * @param jobKey The jobKey to validate.
   * @return {@code true} if the jobKey validates.
   */
  public static boolean isValid(@Nullable IJobKey jobKey) {
    return jobKey != null
        && ConfigurationManager.isGoodIdentifier(jobKey.getRole())
        && ConfigurationManager.isGoodIdentifier(jobKey.getEnvironment())
        && ConfigurationManager.isGoodIdentifier(jobKey.getName());
  }

  /**
   * Assert that a jobKey struct is valid.
   *
   * @param jobKey The key struct to validate.
   * @return The validated jobKey argument.
   * @throws IllegalArgumentException if the key struct fails to validate.
   */
  public static IJobKey assertValid(IJobKey jobKey) throws IllegalArgumentException {
    checkArgument(isValid(jobKey));

    return jobKey;
  }

  /**
   * Attempt to create a valid JobKey from the given (role, environment, name) triple.
   *
   * @param role The job's role.
   * @param environment The job's environment.
   * @param name The job's name.
   * @return A valid JobKey if it can be created.
   * @throws IllegalArgumentException if the key fails to validate.
   */
  public static IJobKey from(String role, String environment, String name)
      throws IllegalArgumentException {

    IJobKey job = IJobKey.build(new JobKey()
        .setRole(role)
        .setEnvironment(environment)
        .setName(name));
    return assertValid(job);
  }

  /**
   * Attempts to create a valid JobKey from the given task.
   *
   * @param task The task to create job key from.
   * @return A valid JobKey if it can be created.
   * @throws IllegalArgumentException if the key fails to validate.
   */
  public static IJobKey from(ITaskConfig task) throws IllegalArgumentException {
    return from(task.getOwner().getRole(), task.getEnvironment(), task.getJobName());
  }

  /**
   * Create a "/"-delimited representation of job key usable as a unique identifier in this cluster.
   *
   * It is guaranteed that {@code k.equals(JobKeys.parse(JobKeys.canonicalString(k))}.
   *
   * @see #parse(String)
   * @param jobKey Key to represent.
   * @return Canonical "/"-delimited representation of the key.
   */
  public static String canonicalString(IJobKey jobKey) {
    return Joiner.on("/").join(jobKey.getRole(), jobKey.getEnvironment(), jobKey.getName());
  }

  /**
   * Create a job key from a "role/environment/name" representation.
   *
   * It is guaranteed that {@code k.equals(JobKeys.parse(JobKeys.canonicalString(k))}.
   *
   * @see #canonicalString(IJobKey)
   * @param string Input to parse.
   * @return Parsed representation.
   * @throws IllegalArgumentException when the string fails to parse.
   */
  public static IJobKey parse(String string) throws IllegalArgumentException {
    List<String> components = Splitter.on("/").splitToList(string);
    checkArgument(components.size() == 3);
    return from(components.get(0), components.get(1), components.get(2));
  }

  /**
   * Attempt to extract job keys from the given query if it is job scoped.
   *
   * @param query Query to extract the keys from.
   * @return A present if keys can be extracted, absent otherwise.
   */
  public static Optional<Set<IJobKey>> from(Query.Builder query) {
    if (Query.isJobScoped(query)) {
      TaskQuery taskQuery = query.get();
      ImmutableSet.Builder<IJobKey> builder = ImmutableSet.builder();

      if (taskQuery.isSetJobName()) {
        builder.add(from(
            taskQuery.getOwner().getRole(),
            taskQuery.getEnvironment(),
            taskQuery.getJobName()));
      }

      if (taskQuery.isSetJobKeys()) {
        builder.addAll(IJobKey.setFromBuilders(taskQuery.getJobKeys()));
      }
      return Optional.of(assertValid(builder.build()));
    } else {
      return Optional.absent();
    }
  }

  private static Set<IJobKey> assertValid(Set<IJobKey> jobKeys)
      throws IllegalArgumentException {

    for (IJobKey jobKey : jobKeys) {
      checkArgument(isValid(jobKey), "Invalid job key format:" + jobKey);
    }

    return jobKeys;
  }
}
