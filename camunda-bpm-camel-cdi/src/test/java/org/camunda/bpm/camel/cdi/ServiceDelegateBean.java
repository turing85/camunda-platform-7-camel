package org.camunda.bpm.camel.cdi;

import java.io.IOException;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.quarkus.core.CamelProducers;
import org.camunda.bpm.camel.component.CamundaBpmConstants;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.jboss.logging.Logger;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID;

@Named
@Dependent
public class ServiceDelegateBean implements JavaDelegate {

    @Inject
    BeanManager beanManager;

    @Inject
    CamelContextBootstrap camelContextBootstrap;

    public static final String VARIABLE_RESULT = "result";

    @Override
    public void execute(DelegateExecution execution) {
        try (ProducerTemplate tpl = camelContextBootstrap.getCamelContext().createProducerTemplate()) {
            tpl.sendBodyAndProperty((String) execution.getVariable("endpoint"),
                    execution.getVariable("content"),
                    CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID, execution.getProcessInstanceId());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
