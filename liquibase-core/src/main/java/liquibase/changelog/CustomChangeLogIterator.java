/*
 * This software is the confidential and proprietary information of ThetaSP S/B
 * ("Confidential Information").
 * You shall not disclose such Confidential Information
 * and shall use it only in accordance with the terms of the
 * license agreement you entered into
 * with ThetaSP S/B.
 */

package liquibase.changelog;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.tools.ant.types.resources.FileResource;
import org.springframework.util.CollectionUtils;

import liquibase.RuntimeEnvironment;
import liquibase.changelog.filter.ChangeSetFilter;
import liquibase.changelog.filter.ChangeSetFilterResult;
import liquibase.changelog.visitor.ChangeSetVisitor;
import liquibase.changelog.visitor.SkippedChangeSetVisitor;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.executor.CustomLoggingExecutor;
import liquibase.executor.ExecutorService;
import liquibase.logging.LogService;
import liquibase.logging.LogType;
import liquibase.logging.Logger;
import liquibase.logging.LoggerContext;

/**
 * Custom {@link CustomChangeLogIterator} that used by Theta personally to override the logic handling
 * on lopping each YAML ChangeSet and generate the relevant SQL files under the lowest directory level
 *
 * @author PJH
 * @version 1.0
 */
public class CustomChangeLogIterator extends ChangeLogIterator {

    // logger for CustomChangelogIterator
    private static final Logger LOG = LogService.getLog(CustomChangeLogIterator.class);

    /**
     * Creates a new CustomChangeLogIterator.java object.
     */
    public CustomChangeLogIterator(DatabaseChangeLog databaseChangeLog,
                                   ChangeSetFilter[] changeSetFilters) {
        super(databaseChangeLog, changeSetFilters);
    }

    /**
     * Creates a new CustomChangeLogIterator.java object.
     */
    public CustomChangeLogIterator(List<RanChangeSet> changeSetList, DatabaseChangeLog changeLog,
                                   ChangeSetFilter... changeSetFilters) {
        super(changeSetList, changeLog, changeSetFilters);
    }

    /**
     * Execute the generation of YAML to SQL
     *
     * @param visitor {@link ChangeSetVisitor}
     * @param env {@link RuntimeEnvironment}
     * @param database {@link Database}
     * @param outputDirectory output directory
     * @param sourceDirectory source directory
     * @param outputFile output file name
     * @param outputEncoding output file encoding
     * @throws LiquibaseException {@link LiquibaseException}
     * @throws UnsupportedEncodingException {@link UnsupportedEncodingException}
     * @throws IOException {@link IOException}
     */
    public void run(ChangeSetVisitor visitor, RuntimeEnvironment env, Database database,
                    String outputDirectory, String sourceDirectory, String outputFile,
                    String outputEncoding)
            throws LiquibaseException, UnsupportedEncodingException, IOException {
        databaseChangeLog.setRuntimeEnvironment(env);
        try (LoggerContext ignored = LogService.pushContext("databaseChangeLog",
                databaseChangeLog)) {
            List<ChangeSet> changeSetList = new ArrayList<>(databaseChangeLog.getChangeSets());
            if (visitor.getDirection().equals(ChangeSetVisitor.Direction.REVERSE)) {
                Collections.reverse(changeSetList);
            }
            LOG.info(LogType.LOG, "Preparing to execute ChangeSet");
            Map<String, List<ChangeSet>> unqChangeSets = unqChangeSets(changeSetList);
            for (Entry<String, List<ChangeSet>> entrySet : unqChangeSets.entrySet()) {
                LOG.info(LogType.LOG, "Looping on ChangeSet");
                CustomLoggingExecutor executor = (CustomLoggingExecutor) ExecutorService
                        .getInstance().getExecutor(database);
                FileResource fileResource = getOutputFileResource(sourceDirectory, outputDirectory,
                        outputFile, entrySet.getKey());
                executor.setOutput(
                        new OutputStreamWriter(fileResource.getOutputStream(), outputEncoding));
                for (ChangeSet changeSet : entrySet.getValue()) {
                    boolean shouldVisit = true;
                    Set<ChangeSetFilterResult> reasonsAccepted = new HashSet<>();
                    Set<ChangeSetFilterResult> reasonsDenied = new HashSet<>();
                    if (changeSetFilters != null) {
                        for (ChangeSetFilter filter : changeSetFilters) {
                            ChangeSetFilterResult acceptsResult = filter.accepts(changeSet);
                            if (acceptsResult.isAccepted()) {
                                reasonsAccepted.add(acceptsResult);
                            } else {
                                shouldVisit = false;
                                reasonsDenied.add(acceptsResult);
                                break;
                            }
                        }
                    }

                    try (LoggerContext ignored2 = LogService.pushContext("changeSet", changeSet)) {
                        if (shouldVisit && !alreadySaw(changeSet)) {
                            visitor.visit(changeSet, databaseChangeLog, env.getTargetDatabase(),
                                    reasonsAccepted);
                            markSeen(changeSet);
                        } else {
                            if (visitor instanceof SkippedChangeSetVisitor) {
                                ((SkippedChangeSetVisitor) visitor).skipped(changeSet,
                                        databaseChangeLog, env.getTargetDatabase(), reasonsDenied);
                            }
                        }
                    }
                }
                executor.getOutput().flush();
            }
        } finally {
            databaseChangeLog.setRuntimeEnvironment(null);
        }
    }

    /**
     * Unique list of ChangeSets to avoid frequent I/O access on OS
     * @param changeSetList list of {@link ChangeSet}
     * @return list of {@link ChangeSet} with their relative path as key
     */
    protected Map<String, List<ChangeSet>> unqChangeSets(List<ChangeSet> changeSetList) {
        Map<String, List<ChangeSet>> unqChangeSets = new HashMap<>();
        for (ChangeSet changeSet : changeSetList) {
            String physicalFilePath = changeSet.getChangeLog().getPhysicalFilePath();
            LOG.info("Physical File Path: " + physicalFilePath);
            String relativeFilePath = physicalFilePath.substring(0,
                    physicalFilePath.lastIndexOf("/") + 1);
            List<ChangeSet> changeSets = unqChangeSets.get(relativeFilePath);
            if (CollectionUtils.isEmpty(changeSets)) {
                changeSets = new ArrayList<>();
                unqChangeSets.put(relativeFilePath, changeSets);
            }
            changeSets.add(changeSet);
        }
        LOG.info(LogType.LOG, "Unique CHnageSet List Size: " + unqChangeSets.size());
        return unqChangeSets;
    }

    /**
     * Retrieve {@link FileResource} by constructing the targeted file path
     * @param sourceDirectory source directory
     * @param outputDirectory output directory
     * @param outputFile output file name
     * @param changeSetRelativeDirectory changeset physical file path
     * @return targeted output file resource {@link FileResource}
     */
    protected FileResource getOutputFileResource(String sourceDirectory, String outputDirectory,
                                                 String outputFile, String changeSetRelativeDirectory) {
        LOG.info("SourceDirectory: " + sourceDirectory);
        String module = sourceDirectory.substring(sourceDirectory.lastIndexOf('/'));
        StringBuilder strBld = new StringBuilder();
        strBld.append(outputDirectory).append(module).append("/").append(changeSetRelativeDirectory)
                .append(outputFile);
        File file = new File(strBld.toString());
        LOG.info("ChangeSet File Path: " + strBld.toString());
        return new FileResource(file);
    }
}