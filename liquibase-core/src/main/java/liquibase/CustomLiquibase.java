/*
 * This software is the confidential and proprietary information of ThetaSP S/B
 * ("Confidential Information").
 * You shall not disclose such Confidential Information
 * and shall use it only in accordance with the terms of the
 * license agreement you entered into
 * with ThetaSP S/B.
 */

package liquibase;

import java.io.IOException;

import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.changelog.ChangeLogIterator;
import liquibase.changelog.CustomChangeLogIterator;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.filter.ChangeSetFilter;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.executor.CustomLoggingExecutor;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.lockservice.LockService;
import liquibase.lockservice.LockServiceFactory;
import liquibase.logging.LogService;
import liquibase.logging.LogType;
import liquibase.logging.Logger;
import liquibase.resource.ResourceAccessor;

/**
 * Custom Liquibase instance that been used for Theta personally.
 *
 * @author PJH
 * @version 1.0
 */
public class CustomLiquibase extends Liquibase {

    private static final Logger LOG = LogService.getLog(Liquibase.class);

    /**
     * Creates a new CustomLiquibase.java object.
     */
    public CustomLiquibase(DatabaseChangeLog changeLog, ResourceAccessor resourceAccessor,
                           Database database) {
        super(changeLog, resourceAccessor, database);
    }

    /**
     * Creates a new CustomLiquibase.java object.
     */
    public CustomLiquibase(String changeLogFile, ResourceAccessor resourceAccessor,
                           Database database) {
        super(changeLogFile, resourceAccessor, database);
    }

    /**
     * Creates a new CustomLiquibase.java object.
     */
    public CustomLiquibase(String changeLogFile, ResourceAccessor resourceAccessor,
                           DatabaseConnection conn) throws LiquibaseException {
        super(changeLogFile, resourceAccessor, conn);
    }

    /**
     * Forward database with lowest directory as the destination for SQL file generation
     *
     * @param contexts {@link Contexts}
     * @param labelExpression {@link LabelExpression}
     * @param outputDirectory output directory
     * @param sourceDirectory source directory
     * @param outputFile output file name with extension
     * @param outputEncoding output file encoding
     * @throws LiquibaseException {@link LiquibaseException}
     * @throws IOException {@link IOException}
     */
    public void updateWithDirectory(Contexts contexts, LabelExpression labelExpression,
                                    String outputDirectory, String sourceDirectory, String outputFile,
                                    String outputEncoding) throws LiquibaseException, IOException {
        getChangeLogParameters().setContexts(contexts);
        getChangeLogParameters().setLabels(labelExpression);

        /* We have no other choice than to save the current Executer here. */
        @SuppressWarnings("squid:S1941")
        Executor oldTemplate = ExecutorService.getInstance().getExecutor(database);
        CustomLoggingExecutor loggingExecutor = new CustomLoggingExecutor(
                ExecutorService.getInstance().getExecutor(database), database);
        ExecutorService.getInstance().setExecutor(database, loggingExecutor);

        LockService lockService = LockServiceFactory.getInstance().getLockService(database);
        lockService.waitForLock();
        //update(contexts, labelExpression, true, outputDirectory, sourceDirectory, outputFile,
        //	outputEncoding);
        try {
            DatabaseChangeLog changeLog = getDatabaseChangeLog();
            checkLiquibaseTables(true, changeLog, contexts, labelExpression);

            ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database)
                    .generateDeploymentId();
            changeLog.validate(database, contexts, labelExpression);

            ChangeLogIterator changeLogIterator = getStandardChangelogIterator(contexts,
                    labelExpression, changeLog);
            ChangeSetFilter[] changeSetFilters = new ChangeSetFilter[changeLogIterator
                    .getChangeSetFilters().size()];
            CustomChangeLogIterator customChangeLogIterator = new CustomChangeLogIterator(changeLog,
                    changeLogIterator.getChangeSetFilters().toArray(changeSetFilters));

            customChangeLogIterator.run(createUpdateVisitor(),
                    new RuntimeEnvironment(database, contexts, labelExpression), database,
                    outputDirectory, sourceDirectory, outputFile, outputEncoding);
        } finally {
            database.setObjectQuotingStrategy(ObjectQuotingStrategy.LEGACY);
            try {
                lockService.releaseLock();
            } catch (LockException e) {
                LOG.severe(LogType.LOG, MSG_COULD_NOT_RELEASE_LOCK, e);
            }
            resetServices();
        }

        ExecutorService.getInstance().setExecutor(database, oldTemplate);
    }

}