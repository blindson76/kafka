package com.uber.data.kafka.connect.distributed.assignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Predicate;

// Hey just FYI this class is absolutely not thread-safe and you're deranged if you try to use it that way
public abstract class AbstractAssignor<G, J> {

    private static final Logger log = LoggerFactory.getLogger(AbstractAssignor.class);

    private final Comparator<G> groupComparator;
    private final Comparator<J> jobComparator;
    private final double groupLoadThreshold;
    private final double defaultJobLoad;
    private final int groupJobLimit;
    private final Map<G, Double> cachedGroupLoads;
    private final Map<J, Double> cachedJobLoads;

    @SuppressWarnings("this-escape")
    protected AbstractAssignor(double groupLoadThreshold, double defaultJobLoad, int groupJobLimit) {
        this.groupComparator = Comparator.comparing(this::cachedGroupLoad);
        this.jobComparator = Comparator.comparing(this::cachedJobLoad);
        this.groupLoadThreshold = groupLoadThreshold;
        this.defaultJobLoad = defaultJobLoad;
        this.groupJobLimit = groupJobLimit;
        this.cachedGroupLoads = new HashMap<>();
        this.cachedJobLoads = new HashMap<>();
    }

    protected abstract Double jobLoad(J job) throws Exception;

    protected abstract void assign(G group, J job);
    protected abstract void revoke(G group, J job);
    protected abstract Collection<J> jobs(G group);

    protected abstract String groupType();
    protected abstract String jobType();

    // Can be overridden by subclasses if the lawless chaos of the English language demands it
    protected String groupTypePlural() {
        return groupType() + "(s)";
    }

    // Can be overridden by subclasses if the lawless chaos of the English language demands it
    protected String jobTypePlural() {
        return jobType() + "(s)";
    }

    protected String jobName(J job) {
        return job.toString();
    }

    protected String groupName(G group) {
        return group.toString();
    }

    // toAdd contains only new jobs that aren't already assigned and which should
    // be assigned to a group if possible
    public void assign(Collection<G> groups, Collection<J> toAdd) {
        log.info("Rebalancing {} {} with {} new {} to assign", groups.size(), groupTypePlural(), toAdd.size(), jobTypePlural());

        cachedGroupLoads.clear();
        cachedJobLoads.clear();

        Collection<J> toAssign = new HashSet<>(toAdd);
        Collection<J> removed = enforceGroupJobLimit(groups);
        toAssign.addAll(removed);

        assignAll(groups, toAssign);

        applyLoadThreshold(groups);

        log.info("Finished rebalancing {} {} with {} new {} to assign", groups.size(), groupTypePlural(), toAdd.size(), jobTypePlural());
    }

    private Collection<J> enforceGroupJobLimit(Collection<G> groups) {
        if (groupJobLimit <= 0) {
            log.debug("Skipping limit enforcement for {} per {} since limit is non-positive", jobTypePlural(), groupType());
            return Set.of();
        }

        log.debug("Enforcing limit for {} per {} across {} {}", jobTypePlural(), groupType(), groups.size(), groupTypePlural());

        Collection<J> result = new ArrayList<>();
        for (G group : groups) {
            List<J> jobs = new ArrayList<>(jobs(group));
            int numJobs = jobs.size();
            int numRevoked = numJobs - groupJobLimit;
            if (numRevoked <= 0) {
                log.trace("{} {} has {} {}, which is within the limit of {}",
                        capitalize(groupType()), groupName(group), numJobs, jobTypePlural(), groupJobLimit
                );
                continue;
            }

            log.debug("{} {} has {} {}, which is above the limit of {}; will revoke {} {}",
                    capitalize(groupType()), groupName(group), numJobs, jobTypePlural(), groupJobLimit, numRevoked, jobTypePlural()
            );
            // Revoke smallest jobs in group
            jobs.sort(jobComparator);
            for (int i = 0; i < numRevoked; i++) {
                J revoked = jobs.get(i);
                log.trace("Revoking {} {} from {} {}", jobType(), jobName(revoked), groupType(), groupName(group));
                revokeAndUpdateLoad(group, revoked);
                result.add(revoked);
            }
        }

        log.debug("Finished enforcing limit for {} per {} across {} {}", jobTypePlural(), groupType(), groups.size(), groupTypePlural());

        return result;
    }

    private void assignAll(Collection<G> groups, Collection<J> toAssign) {
        assert !groups.isEmpty();

        if (toAssign.isEmpty()) {
            log.debug("No new {} to assign", jobTypePlural());
            return;
        }

        log.debug("Assigning {} new {} across {} {}", toAssign.size(), jobTypePlural(), groups.size(), groupTypePlural());

        // Assign largest jobs first
        List<J> sortedJobs = new ArrayList<>(toAssign);
        sortedJobs.sort(jobComparator);

        // Attempt to assign to least-loaded groups first, but only to groups
        // that are under the job limit
        PriorityQueue<G> eligibleGroups = new PriorityQueue<>(groupComparator);
        groups.forEach(group -> {
            if (beneathJobLimit(group)) {
                eligibleGroups.add(group);
            }
        });

        int assigned = 0;
        int assignedPercent = 0;

        for (J job : sortedJobs) {
            G group = eligibleGroups.poll();

            if (group == null) {
                if (groupJobLimit > 0) {
                    // TODO: Metrics
                    log.warn("No {} found to accept {} {}; all {} may be above {} limit of {}",
                            groupTypePlural(), jobType(), jobName(job), groupTypePlural(), jobType(), groupJobLimit
                    );
                } else {
                    log.warn(
                            "No {} found to accept {} {}; this should never happen",
                            groupTypePlural(), jobType(), jobName(job)
                    );
                    assert false; // Fail if we're in a testing environment
                }
                continue;
            }

            log.trace("Assigning {} {} to {} {}", jobType(), jobName(job), groupType(), groupName(group));
            assignAndUpdateLoad(group, job);

            if (beneathJobLimit(group)) {
                boolean added = eligibleGroups.add(group);

                if (!added) {
                    log.warn(
                            "Failed to re-insert {} {} into queue after assigning {} {}; this should never happen",
                            groupType(), groupName(group), jobType(), jobName(job)
                    );
                    assert false; // Fail if we're in a testing environment
                }
            } else {
                log.debug("{} {} has now reached limit of {}; will not assign any more {} to it",
                        capitalize(groupType()), groupName(group), groupJobLimit, jobTypePlural()
                );
            }

            int newAssignedPercent = ++assigned * 100 / toAssign.size();
            if (newAssignedPercent != assignedPercent) {
                assignedPercent = newAssignedPercent;
                log.trace("Assigned {}% of total {} ({} / {})", assignedPercent, jobTypePlural(), assigned, toAssign.size());
            }
        }

        log.debug("Finished assigning {} new {} across {} {}", toAssign.size(), jobTypePlural(), groups.size(), groupTypePlural());
    }

    // TODO: Dynamic load threshold based on ratio of least-loaded group to others?
    //       For example, if loads are 3, 5, 6, 7, 8, and 9, and the threshold is 2, we might
    //       still choose to do some balancing even though all groups are above the threshold;
    //       the heuristic we could use is to try to balance when groups have 1.5x, 2x, 3x, 5x, etc.
    //       the lowest load in the group; if the factor were 2x, we'd try to balance out the groups
    //       with loads 6, 7, 8, and 9; if the factor were 2.5x, we'd try to balance out the groups with
    //       loads 8 and 9; if it were 3x, we'd try to balance out the group with load 9, and if it were
    //       higher, we wouldn't try to do any balancing

    private void applyLoadThreshold(Collection<G> groups) {
        assert !groups.isEmpty();

        log.debug("Applying load threshold across {} {}", groups.size(), groupTypePlural());

        /*
          - Load-balancing reassignment process:
            - Sort groups by load
            - Iterating from most-loaded to least-loaded group:
              - While group is overloaded:
                - Iterating from least-heavy to most-heavy job in group:
                  - Attempt to reassign job to least-loaded group that can accept job
                    without exceeding threshold
                  - If job cannot be reassigned without exceeding threshold, stop trying
                    to take load off of group (and move on to next overloaded group,
                    if there is one)
         */
        List<G> overloadedGroups = groups.stream()
                .filter(this::isOverloaded)
                .sorted(groupComparator.reversed())
                .toList();

        int numOverloaded = overloadedGroups.size();
        log.debug("Found {} overloaded {}", numOverloaded, groupTypePlural());

        if (overloadedGroups.isEmpty())
            return;

        PriorityQueue<G> eligibleGroups = new PriorityQueue<>(groupComparator);
        groups.stream()
                .filter(this::beneathJobLimit)
                .filter(Predicate.not(this::isOverloaded))
                .forEach(eligibleGroups::add);

        log.debug("Found {} eligible {} to receive new {} for load balancing",
                eligibleGroups.size(), groupTypePlural(), jobTypePlural()
        );

        for (G overloaded : overloadedGroups) {
            List<J> sortedJobs = new ArrayList<>(jobs(overloaded));
            sortedJobs.sort(jobComparator);

            for (J job : sortedJobs) {
                G leastLoaded = eligibleGroups.peek();
                if (leastLoaded == null) {
                    return; // Nothing else we can do, every group is overloaded
                }

                if (!canAccept(leastLoaded, job)) {
                    log.trace("Cannot reassign {} {} to {} {} (would become overloaded)",
                            jobType(), jobName(job), groupType(), groupName(leastLoaded));
                    // Least-heavy job in this group cannot be assigned to the least-loaded group;
                    // nothing left we can do
                    break;
                }

                log.trace("Reassigning {} {} from {} {} to {} {}",
                        jobType(), jobName(job), groupType(), groupName(overloaded), groupType(), groupName(leastLoaded));
                // Reassign from the overloaded group to the non-overloaded one
                revokeAndUpdateLoad(overloaded, job);
                assignAndUpdateLoad(leastLoaded, job);

                if (beneathJobLimit(leastLoaded)) {
                    // Re-insert into the queue to reflect new workload
                    eligibleGroups.poll();
                    eligibleGroups.add(leastLoaded);
                } else {
                    log.debug("{} {} is at or above {} limit of {}; will not assign any more {} to it",
                            capitalize(groupType()), groupName(leastLoaded), jobType(), groupJobLimit, jobTypePlural()
                    );
                }

                // Check to see if the group is still overloaded
                if (!isOverloaded(overloaded)) {
                    numOverloaded--;
                    log.debug("{} {} is no longer overloaded", capitalize(groupType()), groupName(overloaded));
                    // If the group is no longer overloaded, we can stop trying to transfer
                    // jobs off of it, and add it to the non-overloaded queue
                    // so that it can potentially receive new jobs from other overloaded groups
                    if (beneathJobLimit(overloaded)) {
                        eligibleGroups.add(overloaded);
                    } else {
                        log.debug("{} {} is at or above {} limit of {}; will not assign any more {} to it",
                                capitalize(groupType()), groupName(overloaded), jobType(), groupJobLimit, jobTypePlural()
                        );
                    }
                    break;
                } else {
                    log.trace("{} {} is still overloaded", capitalize(groupType()), groupName(overloaded));
                }
            }
        }

        log.debug("Finished applying load threshold across {} {}; there are {} overloaded {} remaining",
            groups.size(), groupTypePlural(), numOverloaded, groupTypePlural()
        );
    }

    private boolean canAccept(G group, J job) {
        double newLoad = cachedGroupLoad(group) + cachedJobLoad(job);
        return !isOverloaded(newLoad);
    }

    private boolean isOverloaded(G group) {
        return isOverloaded(cachedGroupLoad(group));
    }

    private double cachedGroupLoad(G group) {
        return cachedGroupLoads.computeIfAbsent(group, g ->
                jobs(g).stream().mapToDouble(this::cachedJobLoad).sum()
        );
    }

    private boolean isOverloaded(double load) {
        return load > groupLoadThreshold;
    }

    private double cachedJobLoad(J job) {
        return cachedJobLoads.computeIfAbsent(job, j -> {
            Double load;
            try {
                load = jobLoad(job);
            } catch (Exception e) {
                log.error("Failed to retrieve load for {} {}; falling back on default load of {}", jobType(), jobName(job), defaultJobLoad);
                return defaultJobLoad;
            }

            if (load == null) {
                log.debug("No load available for {} {}; falling back on default load of {}", jobType(), jobName(job), defaultJobLoad);
                return defaultJobLoad;
            }

            if (load < 0) {
                log.warn("Negative load provided for {} {}; will fall back on default load of {}", jobType(), jobName(job), defaultJobLoad);
                return defaultJobLoad;
            }

            log.trace("Found load of {} for {} {}", load, jobType(), jobName(job));

            return load;
        });
    }

    private void revokeAndUpdateLoad(G group, J job) {
        revoke(group, job);
        double newLoad = cachedGroupLoad(group) - cachedJobLoad(job);
        cachedGroupLoads.put(group, newLoad);
    }

    private void assignAndUpdateLoad(G group, J job) {
        assign(group, job);
        double newLoad = cachedGroupLoad(group) + cachedJobLoad(job);
        cachedGroupLoads.put(group, newLoad);
    }

    private int numJobs(G group) {
        return jobs(group).size();
    }

    private boolean beneathJobLimit(G group) {
        return groupJobLimit <= 0 || numJobs(group) < groupJobLimit;
    }

    private static String capitalize(String s) {
        return (s.substring(0, 1).toUpperCase(Locale.ROOT)) + s.substring(1);
    }
}
