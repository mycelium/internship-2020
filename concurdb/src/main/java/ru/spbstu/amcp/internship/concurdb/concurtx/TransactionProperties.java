package ru.spbstu.amcp.internship.concurdb.concurtx;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.core.NamedThreadLocal;
import org.springframework.transaction.support.TransactionSynchronization;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Contains all the properties of a transaction
 */
@AllArgsConstructor
public class TransactionProperties {

    @Getter @Setter
    private Map<Object, Object> resources;
    @Getter @Setter
    private List<TransactionSynchronization> synchronizations;
    @Getter @Setter
    private String currentTransactionName;
    @Getter @Setter
    private Integer currentTransactionIsolationLevel;
    @Getter @Setter
    private Boolean actualTransactionActive;

}
