/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package controllers.api.v1;

public class JsonKeys {

  // Common keys
  public static final String USERNAME = "username";
  public static final String NAME = "name";
  public static final String SEVERITY = "severity";
  public static final String ID = "id";
  public static final String RESOURCE_USED = "resourceused";
  public static final String RESOURCE_WASTED = "resourcewasted";
  public static final String RUNTIME = "runtime";
  public static final String WAITTIME = "waittime";
  public static final String START_TIME = "starttime";
  public static final String FINISH_TIME = "finishtime";
  public static final String COUNT = "count";
  public static final String VALUE = "value";
  public static final String QUEUE = "queue";
  public static final String SEARCH_OPTS = "search-options";
  public static final String START = "start";
  public static final String END = "end";
  public static final String total = "total";
  public static final String SEARCH_RESULTS = "search-results";
  public static final String JOB_TYPES = "jobtypes";
  public static final String HEURISTICS = "heuristics";
  public static final String SEVERITIES = "severities";
  public static final String JOB_CATEGORY = "jobcategory";
  public static final String TOTAL = "total";
  public static final String SUMMARIES = "summaries";

  // Workflows
  public static final String WORKFLOW_SUMMARIES = "workflow-summaries";
  public static final String WORKFLOWS = "workflows";
  public static final String FLOW_DEF_ID = "flowdefid";
  public static final String FLOW_EXEC_ID = "flowexecid";

  // Jobs
  public static final String JOB_SUMMARIES = "job-summaries";
  public static final String JOBS_SEVERITY = "jobsseverity";
  public static final String JOB_EXEC_ID = "jobexecid";
  public static final String JOB_DEF_ID = "jobdefid";
  public static final String JOB_TYPE = "jobtype";
  public static final String JOB_NAME = "jobname";
  public static final String JOBS = "jobs";
  public static final String JOBSSUMMARIES = "jobssummaries";

  // Tasks
  public static final String APPLICATION_SUMMARIES = "application-summaries";
  public static final String HEURISTICS_SUMMARY = "heuristicsummary";
  public static final String TASKS_SEVERITY = "tasksseverity";
  public static final String TASKS_SUMMARIES = "taskssummaries";
  public static final String MAPREDUCE_JOB_NAME = "mapreducejobname";
  public static final String TRACKING_URL = "trackingurl";
  public static final String DETAILS = "details";
  public static final String YARN_APP_HEURISTIC_RESULTS = "yarnappheuristicresults";
  public static final String APPLICATIONS = "applications";
}
