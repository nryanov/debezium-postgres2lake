package jakarta.servlet;

// Trick from https://issues.apache.org/jira/browse/SPARK-51434?focusedCommentId=18007570&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-18007570
// This allows to use spark with new jakarta version (6.x.x)
// https://issues.apache.org/jira/browse/SPARK-51434
@Deprecated
public interface SingleThreadModel {
}