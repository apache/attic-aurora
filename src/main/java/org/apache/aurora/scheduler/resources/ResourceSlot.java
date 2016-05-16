/**
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
package org.apache.aurora.scheduler.resources;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.base.Numbers;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.Builder;
import org.apache.mesos.Protos.TaskInfo;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.common.quantity.Data.BYTES;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;

/**
 * Represents a single task/host aggregate resource vector unaware of any Mesos resource traits.
 */
public final class ResourceSlot {

  private final double numCpus;
  private final Amount<Long, Data> disk;
  private final Amount<Long, Data> ram;
  private final int numPorts;

  /**
   * Empty ResourceSlot value.
   */
  public static final ResourceSlot NONE =
      new ResourceSlot(0, Amount.of(0L, Data.BITS), Amount.of(0L, Data.BITS), 0);

  public ResourceSlot(
      double numCpus,
      Amount<Long, Data> ram,
      Amount<Long, Data> disk,
      int numPorts) {

    this.numCpus = numCpus;
    this.ram = requireNonNull(ram);
    this.disk = requireNonNull(disk);
    this.numPorts = numPorts;
  }

  /**
   * Extracts the resources required from a task.
   *
   * @param task Task to get resources from.
   * @return The resources required by the task.
   */
  public static ResourceSlot from(ITaskConfig task) {
    requireNonNull(task);
    return new ResourceSlot(
        task.getNumCpus(),
        Amount.of(task.getRamMb(), Data.MB),
        Amount.of(task.getDiskMb(), Data.MB),
        Iterables.size(ResourceManager.getTaskResources(task, PORTS)));
  }

  /**
   * Ensures that the revocable setting on the executor and task CPU resources match.
   *
   * @param task Task to check for resource type alignment.
   * @return A possibly-modified task, with aligned CPU resource types.
   */
  public static TaskInfo matchResourceTypes(TaskInfo task) {
    TaskInfo.Builder taskBuilder = task.toBuilder();

    Optional<Resource> revocableTaskCpu = taskBuilder.getResourcesList().stream()
        .filter(r -> r.getName().equals(CPUS.getMesosName()))
        .filter(Resource::hasRevocable)
        .findFirst();
    ExecutorInfo.Builder executorBuilder = taskBuilder.getExecutorBuilder();

    Consumer<Builder> matchRevocable = builder -> {
      if (revocableTaskCpu.isPresent()) {
        builder.setRevocable(revocableTaskCpu.get().getRevocable());
      } else {
        builder.clearRevocable();
      }
    };

    executorBuilder.getResourcesBuilderList().stream()
        .filter(r -> r.getName().equals(CPUS.getMesosName()))
        .forEach(matchRevocable);

    return taskBuilder.build();
  }

  /**
   * Convenience method for adapting to Mesos resources without applying a port range.
   *
   * @param tierInfo Task tier info.
   * @return Mesos resources.
   */
  public List<Protos.Resource> toResourceList(TierInfo tierInfo) {
    return ImmutableList.<Protos.Resource>builder()
        .add(makeMesosResource(CPUS, numCpus, tierInfo.isRevocable()))
        .add(makeMesosResource(DISK_MB, disk.as(Data.MB), false))
        .add(makeMesosResource(RAM_MB, ram.as(Data.MB), false))
        .build();
  }

  /**
   * Creates a mesos resource of integer ranges.
   *
   * @param resourceType Resource type.
   * @param values    Values to translate into ranges.
   * @return A new mesos ranges resource.
   */
  @VisibleForTesting
  public static Protos.Resource makeMesosRangeResource(
      ResourceType resourceType,
      Set<Integer> values) {

    return Protos.Resource.newBuilder()
        .setName(resourceType.getMesosName())
        .setType(Protos.Value.Type.RANGES)
        .setRanges(Protos.Value.Ranges.newBuilder()
            .addAllRange(Iterables.transform(Numbers.toRanges(values), Numbers.RANGE_TRANSFORM)))
        .build();
  }

  /**
   * Creates a scalar mesos resource.
   *
   * @param resourceType Resource type.
   * @param value Value for the resource.
   * @param revocable Flag indicating if this resource is revocable.
   * @return A mesos resource.
   */
  @VisibleForTesting
  static Protos.Resource makeMesosResource(
      ResourceType resourceType,
      double value,
      boolean revocable) {

    Protos.Resource.Builder builder = Protos.Resource.newBuilder()
        .setName(resourceType.getMesosName())
        .setType(Protos.Value.Type.SCALAR)
        .setScalar(Protos.Value.Scalar.newBuilder().setValue(value));

    if (revocable) {
      builder.setRevocable(Protos.Resource.RevocableInfo.newBuilder());
    }

    return builder.build();
  }

  /**
   * Number of CPUs.
   *
   * @return CPUs.
   */
  public double getNumCpus() {
    return numCpus;
  }

  /**
   * Disk amount.
   *
   * @return Disk.
   */
  public Amount<Long, Data> getDisk() {
    return disk;
  }

  /**
   * RAM amount.
   *
   * @return RAM.
   */
  public Amount<Long, Data> getRam() {
    return ram;
  }

  /**
   * Number of ports.
   *
   * @return Port count.
   */
  public int getNumPorts() {
    return numPorts;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ResourceSlot)) {
      return false;
    }

    ResourceSlot other = (ResourceSlot) o;
    return Objects.equals(numCpus, other.numCpus)
        && Objects.equals(ram, other.ram)
        && Objects.equals(disk, other.disk)
        && Objects.equals(numPorts, other.numPorts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(numCpus, ram, disk, numPorts);
  }

  /**
   * Sums up all resources in {@code slots}.
   *
   * @param slots Resource slots to sum up.
   * @return Sum of all resource slots.
   */
  public static ResourceSlot sum(Iterable<ResourceSlot> slots) {
    ResourceSlot sum = NONE;

    for (ResourceSlot r : slots) {
      sum = sum.add(r);
    }

    return sum;
  }

  /**
   * Adds {@code other}.
   *
   * @param other Resource slot to add.
   * @return Result.
   */
  public ResourceSlot add(ResourceSlot other) {
    return new ResourceSlot(
        getNumCpus() + other.getNumCpus(),
        Amount.of(getRam().as(BYTES) + other.getRam().as(BYTES), BYTES),
        Amount.of(getDisk().as(BYTES) + other.getDisk().as(BYTES), BYTES),
        getNumPorts() + other.getNumPorts());
  }

  /**
   * Subtracts {@code other}.
   *
   * @param other Resource slot to subtract.
   * @return Result.
   */
  public ResourceSlot subtract(ResourceSlot other) {
    return new ResourceSlot(
        getNumCpus() - other.getNumCpus(),
        Amount.of(getRam().as(BYTES) - other.getRam().as(BYTES), BYTES),
        Amount.of(getDisk().as(BYTES) - other.getDisk().as(BYTES), BYTES),
        getNumPorts() - other.getNumPorts());
  }
}
