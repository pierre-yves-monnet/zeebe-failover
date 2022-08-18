package org.camunda.failover;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.spring.client.annotation.ZeebeWorker;
import org.camunda.cherry.definition.AbstractWorker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Random;

@Component
public class FailOverWorker extends AbstractWorker {

    @Autowired
    ZeebeClient zeebeClient;


    private static final String INPUT_MAXRETRIES = "maxRetries";
    public static final String WORKERTYPE_FAILOVER = "failover";

    public FailOverWorker() {
        super(WORKERTYPE_FAILOVER,
                Collections.singletonList(
                        WorkerParameter.getInstance(INPUT_MAXRETRIES, Long.class, Level.OPTIONAL,
                                "Max retry: if set, override the number of retry given in the process")
                ),
                Collections.emptyList(),
                Collections.emptyList());
    }

    /** We put the logic here, because we want to manage the result of the worker (failed, correct)
     * Cherry framework does not allow to manage this concept: worker is supposed to not failed and return BPMN ERROR
     * @param jobClient Job client
     * @param activatedJob activated job for
     */
    @Override
    @ZeebeWorker(type = WORKERTYPE_FAILOVER)
    public void handleWorkerExecution(final JobClient jobClient, final ActivatedJob activatedJob) {

        Long maxRetries = getInputLongValue(INPUT_MAXRETRIES, null, activatedJob);
        // if the Max retrie is set, then the worker want to manage this information by himself.
        // else, we rely on the process information (on the task, parameters "retries"
        if (maxRetries !=null) {
            logInfo("Worker update the maxRetry to ["+maxRetries+"]");

            // set the timout too: if the worker failed, please wait 30 seconds before submit the jobs again
           zeebeClient.newUpdateRetriesCommand(activatedJob.getKey())
                    .retries(maxRetries.intValue())
                    .requestTimeout(Duration.ofMillis(1000L*3L));

        }

        // We want to randomely failed
        Random random = new Random(System.currentTimeMillis());
        int diceRoll=random.nextInt(100);
        logInfo("Worker diceRoll ["+diceRoll+"] - getRetry "+activatedJob.getRetries());

        if (diceRoll<=98) {
            // we failed !
            jobClient.newFailCommand(activatedJob)
                    .retries(activatedJob.getRetries()-1) // <1>: Decrement retries
                    .errorMessage("Could not execute command ") // <2>
                    .send();
            logInfo("FAIL Command pid=["+activatedJob.getProcessInstanceKey()+"] ElementId=["+activatedJob.getElementId()+"]");

            return;
        }

        // we execute the job correctly
        jobClient.newCompleteCommand(activatedJob.getKey()).send().join();
        logInfo("Complete Command  pid="+activatedJob.getProcessInstanceKey()+"] ElementId=["+activatedJob.getElementId()+"]");

    }


    /**
     * @param jobClient        client
     * @param activatedJob     job activated
     * @param contextExecution context of this execution
     */
    @Override
    public void execute(final JobClient jobClient, final ActivatedJob activatedJob, ContextExecution contextExecution) {
        // this method is not called, the class handle directly the management
    }

}
