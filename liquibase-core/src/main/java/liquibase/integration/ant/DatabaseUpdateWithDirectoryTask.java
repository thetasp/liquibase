package liquibase.integration.ant;

import java.io.IOException;

import org.apache.tools.ant.BuildException;

import liquibase.Contexts;
import liquibase.CustomLiquibase;
import liquibase.Liquibase;
import liquibase.exception.LiquibaseException;
import liquibase.util.StringUtils;

/**
 * Ant task for migrating a database forward without consolidating all the SQL into single files
 * Instead, this class will generate all the SQL as per the lowest directory.
 *
 * @author pjh
 * @version 1.0
 */
public class DatabaseUpdateWithDirectoryTask extends AbstractChangeLogBasedTask {

    private boolean dropFirst;
    private String toTag;

    // SQL files output directory
    private String outputDirectory;

    // YAML files sources directory
    private String sourceDirectory;

    /**
     * @see #outputDirectory
     * @return output directory
     */
    public String getOutputDirectory() {
        return outputDirectory;
    }

    /**
     * @see #outputDirectory
     * @param outputDirectory output directory
     */
    public void setOutputDirectory(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    /**
     * @see #sourceDirectory
     * @return source directory
     */
    public String getSourceDirectory() {
        return sourceDirectory;
    }

    /**
     * @see #sourceDirectory
     * @param sourceDirectory source directory
     */
    public void setSourceDirectory(String sourceDirectory) {
        this.sourceDirectory = sourceDirectory;
    }

    /**
     * @see liquibase.integration.ant.BaseLiquibaseTask#executeWithLiquibaseClassloader()
     */
    @Override
    public void executeWithLiquibaseClassloader() throws BuildException {
        Liquibase liquibase = getLiquibase();
        CustomLiquibase customLiquibase = new CustomLiquibase(liquibase.getChangeLogFile(),
                liquibase.getResourceAccessor(), liquibase.getDatabase());
        try {
            String outputDirectory = getOutputDirectory();
            if (StringUtils.isNotEmpty(outputDirectory)) {
                customLiquibase.updateWithDirectory(new Contexts(getContexts()), getLabels(),
                        outputDirectory, getSourceDirectory(), getOutputFile().getName(),
                        getOutputEncoding());
            }

        } catch (LiquibaseException | IOException e) {
            throw new BuildException("Unable to update database. " + e.toString(), e);
        }
    }

    public boolean isDropFirst() {
        return dropFirst;
    }

    public void setDropFirst(boolean dropFirst) {
        this.dropFirst = dropFirst;
    }

    public String getToTag() {
        return toTag;
    }

    public void setToTag(String toTag) {
        this.toTag = toTag;
    }
}