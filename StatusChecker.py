from builtins import NameError
import requests
from datetime import *
import sys
import argparse
import json
import time
import math
import pandas as pd
import openpyxl


# creating parser for input parameters
def createParser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--Env', choices=["int", "dev", "live", "uat"], default='live')
    parser.add_argument('-c', '--Check',
                        choices=["Reports", "Sherlock", "Delivery", "Package", "Conductor", "All"], default='Reports')
    parser.add_argument('-d', '--Days',  default=14, type=int)
    parser.add_argument('--DelayInMs', default=0, type=int)
    parser.add_argument('-w', '--wait', default=False)
    return parser

def GetNewDate(CreatedDateString, OriginalDateString):
    CreateDate = datetime.strptime(CreatedDateString, "%Y-%m-%dT%H:%M:%S.%f")
    OriginalDate = datetime.strptime(OriginalDateString, "%Y-%m-%dT%H:%M:%S")
    DiffMinutes =  timedelta(seconds=(OriginalDate-CreateDate).total_seconds())
    return (datetime.now() + DiffMinutes).strftime("%Y-%m-%dT%H:%M:%SZ")

def PrettyPrint(LOG):
    log = "|   "+LOG+"   |"
    Stars = '*' * len(log)
    result = Stars+"\n"+log+"\n"+Stars
    print(result)

def PrintError(Summary, JobID, TaskLink, ExtraInfo, Env):
    log = "\n"+Summary+"\n"
    OrderLink = "Job Url: http://one-orders-ui.service.owf-"+Env+"/delivery-jobs/"+JobID+"\n"
    TaskLink = "Check link: "+TaskLink+"\n"
    Problem = "Additional info: "+ExtraInfo+"\n"
    count = len(max([log, OrderLink, TaskLink, Problem], key=len))
    line = '='*count
    result = line+log+OrderLink+TaskLink+Problem+line+"\n"
    print(result)
    return result

def PrintWarn(Summary):
    log = "|   "+Summary+"   |"
    Stars = '-' * len(log)
    result = Stars+"\n"+log+"\n"+Stars+"\n"
    #print(result)
    return result

def AddToResult(Result, temp):
    Result["errors"] = Result["errors"] + temp["errors"]
    Result["warn"] = Result["warn"] + temp["warn"]
    Result["Requests"] = Result["Requests"] + temp["Requests"]
    print("Errors: "+str(len(Result['errors'])))
    print("Warn: "+str(len(Result['warn'])))
    print("Requests count:"+str(Result['Requests']))
    return Result

def SearchDeliveryJobs(Payload, Env):
    apiHost = "http://one-orders-api.service.owf-" + Env
    headers_ = {'content-type': 'application/json'}
    timeOut_ = 30
    Jobs = requests.post(apiHost + "/api/v2/jobs/search", data=Payload, timeout=timeOut_, headers=headers_)
    response = Jobs.json()
    return response["records"]

def GetWorkflowInfo(WorkflowID, Env):
    apiHost = "http://conductor-server.service.owf-" + Env
    headers_ = {'content-type': 'application/json'}
    timeOut_ = 30
    Jobs = requests.get(apiHost + "/api/workflow/"+WorkflowID+"?includeTasks=false", timeout=timeOut_, headers=headers_)
    response = Jobs.json()
    return response

def GetPackageJob(urn, Env):
    apiHost = "http://one-packaging-api.service.owf-" + Env
    headers_ = {'content-type': 'application/json'}
    timeOut_ = 30
    Jobs = requests.get(apiHost + "/v1/package/"+urn, timeout=timeOut_, headers=headers_)
    response = Jobs.json()
    return response

def GetCanceledJobsOnOrders(namespace):
    StartDate = datetime.today() - timedelta ( days = namespace.Days )
    Result = []
    payload = '{"searchConditions":[{"propertyName":"type","operator":"eq","value":"Delivery",' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"status","operator":"eq","value":"CANCELED",' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"createdAt","operator":"gt","value":"'+StartDate.strftime("%Y-%m-%dT%H:%M:%S")+'",' \
                                '"nextConditionOperator":"And"}],' \
                                '"sortBy":"orderHrId",' \
                                '"sortDirection":"Desc",' \
                                '"offset":1,' \
                                '"limit":100,' \
                                '"includeProgress":false,' \
                                '"includeBillingInfo":false,' \
                                '"includeRankings":false,' \
                                '"includeRankingsWorkflows":false,' \
                                '"includeRankingsProgress":false,' \
                                '"includeRankingsLanguages":false,' \
                                '"includeRankingsLinks":false,' \
                                '"includeLinks":false,' \
                                '"includeAttachments":false,' \
                                '"includeRankingOverrides":false,' \
                                '"includeQcSignatures":false,' \
                                '"includeDeliverableComponents":false}'
    while True:
        DeliveryJobs = SearchDeliveryJobs(payload, namespace.Env)
        Result = Result + DeliveryJobs
        data = json.loads(payload)
        data["offset"] = data["offset"] + 100
        payload = json.dumps(data)
        time.sleep(namespace.DelayInMs / 1000)
        if len(DeliveryJobs) != 100:
            break
    return Result

def GetPackageJobsOnOrders(namespace):
    StartDate = datetime.today () - timedelta ( days = namespace.Days )
    Result = []
    payload = '{"searchConditions":[{"propertyName":"type","operator":"eq","value":"Delivery",' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"stage","operator":"eq","value":"DELIVERED",' \
              '"nextConditionOperator":"Or"},' \
              '{"propertyName":"stage","operator":"eq","value":"DELIVERING",' \
              '"nextConditionOperator":"Or"},'\
              '{"propertyName":"stage","operator":"eq","value":"READY_FOR_DELIVERY",' \
              '"nextConditionOperator":"Or"},' \
              '{"propertyName":"stage","operator":"eq","value":"PACKAGING",' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"status","operator":"neq","value":"CANCELED",' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"createdAt","operator":"gt","value":"'+StartDate.strftime("%Y-%m-%dT%H:%M:%S")+'",' \
                                '"nextConditionOperator":"And"}],' \
                                '"sortBy":"orderHrId",' \
                                '"sortDirection":"Desc",' \
                                '"offset":1,' \
                                '"limit":100,' \
                                '"includeProgress":false,' \
                                '"includeBillingInfo":false,' \
                                '"includeRankings":true,' \
                                '"includeRankingsWorkflows":false,' \
                                '"includeRankingsProgress":false,' \
                                '"includeRankingsLanguages":false,' \
                                '"includeRankingsLinks":false,' \
                                '"includeLinks":false,' \
                                '"includeAttachments":false,' \
                                '"includeRankingOverrides":false,' \
                                '"includeQcSignatures":false,' \
                                '"includeDeliverableComponents":false}'
    while True:
        DeliveryJobs = SearchDeliveryJobs(payload, namespace.Env)
        Result = Result + DeliveryJobs
        data = json.loads(payload)
        data["offset"] = data["offset"] + 100
        payload = json.dumps(data)
        time.sleep(namespace.DelayInMs / 1000)
        if len(DeliveryJobs) != 100:
            break
    return Result

def GetInProgressJobsOnOrders(namespace):
    StartDate = datetime.today () - timedelta ( days = namespace.Days )
    Result = []
    #Ignoring TRANSCODE_PASS_THROUGH MANUAL_PASS_THROUGH and allowDelivery
    payload = '{"searchConditions":[{"propertyName":"type","operator":"eq","value":"Delivery",' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"status","operator":"eq","value":"IN_PROGRESS",' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"deliveryJobType","operator":"neq","value":"TRANSCODE_PASS_THROUGH",' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"deliveryJobType","operator":"neq","value":"MANUAL_PASS_THROUGH",' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"allowDelivery","operator":"eq","value":true,' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"createdAt","operator":"gt","value":"'+StartDate.strftime("%Y-%m-%dT%H:%M:%S")+'",' \
              '"nextConditionOperator":"And"}],' \
              '"sortBy":"orderHrId",' \
              '"sortDirection":"Desc",' \
              '"offset":1,' \
              '"limit":100,' \
              '"includeProgress":false,' \
              '"includeBillingInfo":false,' \
              '"includeRankings":true,' \
              '"includeRankingsWorkflows":true,' \
              '"includeRankingsProgress":false,' \
              '"includeRankingsLanguages":false,' \
              '"includeRankingsLinks":false,' \
              '"includeLinks":false,' \
              '"includeAttachments":false,' \
              '"includeRankingOverrides":false,' \
              '"includeQcSignatures":false,' \
              '"includeDeliverableComponents":false}'
    while True:
        DeliveryJobs = SearchDeliveryJobs(payload, namespace.Env)
        Result = Result + DeliveryJobs
        data = json.loads(payload)
        data["offset"] = data["offset"] + 100
        payload = json.dumps(data)
        time.sleep(namespace.DelayInMs / 1000)
        if len(DeliveryJobs) != 100:
            break
    return Result

def GetDeliveryJobsOnOrders(namespace):
    StartDate = datetime.today () - timedelta ( days = namespace.Days )
    Result = []
    payload = '{"searchConditions":[{"propertyName":"type","operator":"eq","value":"Delivery",' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"status","operator":"eq","value":"COMPLETED",' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"finalStage","operator":"eq","value": null,' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"createdAt","operator":"gt","value":"'+StartDate.strftime("%Y-%m-%dT%H:%M:%S")+'",' \
              '"nextConditionOperator":"And"}],' \
              '"sortBy":"orderHrId",' \
              '"sortDirection":"Desc",' \
              '"offset":1,' \
              '"limit":100,' \
              '"includeProgress":false,' \
              '"includeBillingInfo":false,' \
              '"includeRankings":true,' \
              '"includeRankingsWorkflows":true,' \
              '"includeRankingsProgress":false,' \
              '"includeRankingsLanguages":false,' \
              '"includeRankingsLinks":false,' \
              '"includeLinks":false,' \
              '"includeAttachments":false,' \
              '"includeRankingOverrides":false,' \
              '"includeQcSignatures":false,' \
              '"includeDeliverableComponents":false}'
    while True:
        DeliveryJobs = SearchDeliveryJobs(payload, namespace.Env)
        Result = Result + DeliveryJobs
        data = json.loads(payload)
        data["offset"] = data["offset"] + 100
        payload = json.dumps(data)
        time.sleep(namespace.DelayInMs / 1000)
        if len(DeliveryJobs) != 100:
            break
    return Result

def GetSherlockJobbyID(JobID, Env):
    apiHost = "http://sherlock.service.owf-" + Env
    headers_ = {'content-type': 'application/json'}
    timeOut_ = 30
    Jobs = requests.get(apiHost + "/v2/job/status/"+JobID, timeout=timeOut_, headers=headers_)
    response = Jobs.json()
    return response

def GetDeliveryJobsOnDelivery(JobUrn, ENV):
    apiHost = "http://one-delivery-api.service.owf-" + ENV
    headers_ = {'content-type': 'application/json'}
    timeOut_ = 30
    Jobs = requests.get(apiHost + "/v1/delivery/"+JobUrn, timeout=timeOut_, headers=headers_)
    response = Jobs.json()
    return response

def CheckInprogressJobsSherlock(namespace):
    Jobs = GetInProgressJobsOnOrders(namespace)
    ExpectedStatuses = ['Complete', 'Canceled', 'In Progress', "Ready For Transcode"]
    Warn = []
    Err = []
    for job in Jobs:
        time.sleep(namespace.DelayInMs / 1000)
        RankingStatuses = []
        SherlockJob = GetSherlockJobbyID(job["id"], namespace.Env)
        for ranking in SherlockJob:
            if ranking["status"] is None:
                Warn.append(PrintWarn("Null status on Sherlock for some Rankings => JobId:" + job["id"]))
                break
            RankingStatuses.append(ranking["status"])
            if ranking["status"] not in ExpectedStatuses and ranking["status"] is not None:
                Err.append(PrintError("Unexpected status for In Progress job:", job["id"],
                            "http://sherlock.service.owf-"+namespace.Env+"/v2/job/status/"+job["id"],
                            "Ranking status: "+str(ranking["status"]),
                            namespace.Env))

        if 'In Progress' not in RankingStatuses \
                and RankingStatuses != []\
                and 'Ready For Transcode' not in RankingStatuses\
                and job["stage"] != 'READY_FOR_DELIVERY':
            Err.append(PrintError("There is no In progress ranking for In Progress job:", job["id"],
                            "http://sherlock.service.owf-"+namespace.Env+"/v2/job/status/"+job["id"],
                            "Job stage: "+job["stage"],
                            namespace.Env))
    return {"errors": Err, "warn": Warn, "Requests": len(Jobs)}

def CheckCancelledJobsSherlock(namespace):
    Jobs = GetCanceledJobsOnOrders(namespace)
    Warn = []
    Err = []
    for job in Jobs:
        time.sleep(namespace.DelayInMs / 1000)
        SherlockJob = GetSherlockJobbyID(job["id"], namespace.Env);
        if 'warning' in SherlockJob:
            Warn.append(PrintWarn("Missed Job on Sherlock => JobId:"+job["id"]))
        else:
            for ranking in SherlockJob:
                if ranking["status"] != 'Canceled' and ranking["status"] != 'Complete':
                    Err.append(PrintError("Missmatch for canceled job:", job["id"],
                               "http://sherlock.service.owf-"+namespace.Env+"/v2/job/status/"+job["id"],
                               "Ranking status: "+str(ranking["status"]),
                               namespace.Env))
        #checks
    return {"errors": Err, "warn": Warn, "Requests": len(Jobs)}

def CheckDeliveredJobsSherlock(namespace):
    Jobs = GetDeliveryJobsOnOrders(namespace)
    ExpectedStatuses = ['Complete', 'Canceled']
    Warn = []
    Err = []
    for job in Jobs:
        RankingStatuses = []
        time.sleep(namespace.DelayInMs / 1000)
        SherlockJob = GetSherlockJobbyID(job["id"], namespace.Env);
        for ranking in SherlockJob:
            if ranking["status"] is None:
                Warn.append(PrintWarn("Null status on Sherlock for some Rankings => JobId:" + job["id"]))
                break;
            RankingStatuses.append(ranking["status"])
            if ranking["status"] not in ExpectedStatuses and ranking["status"] is not None:
                Err.append(PrintError("Unexpected status for Complete job:", job["id"],
                            "http://sherlock.service.owf-"+namespace.Env+"/v2/job/status/"+job["id"],
                            "Ranking status: "+str(ranking["status"]),
                            namespace.Env))

        if 'Complete' not in RankingStatuses and RankingStatuses != []:
            if job["statusDetails"] is None:
                Err.append(PrintError("There is no Complete ranking for Complete job:", job["id"],
                            "http://sherlock.service.owf-"+namespace.Env+"/v2/job/status/"+job["id"],
                            "None",
                            namespace.Env))
            else:
                Warn.append(PrintWarn("Most likely Complete Job has been forced  => JobId:" + job["id"]))

    return {"errors": Err, "warn": Warn, "Requests": len(Jobs)}

def DownloadReport(StartDate, EndDate, Filename, Env):
    apiHost = "http://one-orders-reports-api.service.owf-" + Env
    headers_ = {'content-type': 'application/json'}
    Payload = '{\
      "from": "'+StartDate+'",\
      "to": "'+EndDate+'",\
      "orderBy": "orderHrId",\
      "orderDirection": "DESC"\
        }'
    timeOut_ = 2000
    data = requests.post(apiHost + "/v1/fulfillmentReport/exportToExcelByDateRange",
                         data=Payload,
                         timeout=timeOut_,
                         headers=headers_,
                         stream=True)
    with open(Filename+".xlsx", 'wb') as f:
        for ch in data:
            f.write(ch)

def ConvertToCsv(filename):
    ## opening the xlsx file
    xlsx = openpyxl.load_workbook(filename+'.xlsx')
    ## opening the active sheet
    sheet = xlsx.active
    ## getting the data from the sheet
    data = sheet.rows
    ## creating a csv file
    csv = open(filename+".csv", "w+",  encoding="utf-8")
    for row in data:
        l = list(row)
        for i in range(len(l)):
            if i == len(l) - 1:
                csv.write(str(l[i].value).replace(";", ":"))
            else:
                csv.write(str(l[i].value).replace(";", ":") + ';')
        csv.write('\n')
    ## close the csv file
    csv.close()
    Jobs = pd.read_csv(filename+".csv", sep=';', error_bad_lines=False)
    Jobs.to_json(filename+".json", orient="records", date_format="epoch", double_precision=10,
                     force_ascii=True, date_unit="ms", default_handler=None)
    with open(filename+".json") as json_file:
        data = json.load(json_file)
    return data

def SherlockCheck(Result, namespace):
    PrettyPrint("Start Sherlock Checks")
    print("Start Checks for In Progress jobs")
    Result = AddToResult(Result, CheckInprogressJobsSherlock(namespace))
    print("Finish Checks for In Progress jobs")
    print("Start Checks for Cancelled jobs")
    Result = AddToResult(Result, CheckCancelledJobsSherlock(namespace))
    print("Finish Checks for Cancelled jobs")
    print("Start Checks for Delivered jobs")
    Result = AddToResult(Result, CheckDeliveredJobsSherlock(namespace))
    print("Finish Checks for Delivered jobs")
    PrettyPrint("Finish Sherlock Checks")
    return Result

def DeliveryCheck(Result, namespace):
    PrettyPrint("Start Delivery Checks")
    Jobs = GetDeliveryJobsOnOrders(namespace)
    for job in Jobs:
        metadata = job["metadata"]
        if "deliveryJobId" in metadata.keys():
            DeliveryJob = GetDeliveryJobsOnDelivery(metadata["deliveryJobId"], namespace.Env)
            time.sleep(namespace.DelayInMs / 1000)
            DeliveryJobUrnString = "urn:deluxe:one-orders:deliveryjob:"+job["id"]
            if DeliveryJobUrnString not in DeliveryJob["urns"] or DeliveryJob["deliveryJobId"] != DeliveryJobUrnString:
                Result["errors"].append(PrintError("Delivery job is not linked on Delivery side:", job["id"],
                        "http://one-delivery-api.service.owf-"+namespace.Env+"/v1/delivery/"+metadata["deliveryJobId"],
                        DeliveryJobUrnString,
                        namespace.Env))
            teststring = "urn:deluxe:one-orders:deliveryjob:"
            count = 0
            for urn in DeliveryJob["urns"]:
                if teststring in urn:
                    count = count + 1
            if count != 1:
                Result["errors"].append(PrintError("Delivery job has several links:", job["id"],
                        "http://one-delivery-api.service.owf-"+namespace.Env+"/v1/delivery/"+metadata["deliveryJobId"],
                        DeliveryJobUrnString,
                        namespace.Env))
        else:
            Result["warn"].append(PrintWarn("Delivered job has no urns: "+job["id"]))
    #Result = AddToResult(Result, Temp)
    Result["Requests"] = Result["Requests"]+len(Jobs)
    PrettyPrint("Finish Delivery Checks")
    return Result

def PackageCheck(Result, namespace):
    PrettyPrint("Start Package Checks")
    Jobs = GetPackageJobsOnOrders(namespace)
    print(len(Jobs))
    for job in Jobs:
        ExpectedRankings = []
        UnExpectedRankings = []
        metadata = job["metadata"]
        if "packagingJobId" in metadata.keys():
            for delivery in job["deliverables"]:
                if delivery["isNonDeliverable"] or delivery["status"] == "CANCELED":
                    UnExpectedRankings.append(delivery["urn"])
                else:
                    ExpectedRankings.append(delivery["urn"])
            if len(ExpectedRankings) == 0:
                Result["warn"].append(PrintWarn("Delivered job has no Rankings for package: "+job["id"]))
            try:
                PackageJob = GetPackageJob(metadata["packagingJobId"], namespace.Env)
                time.sleep(namespace.DelayInMs / 1000)
                for urn in PackageJob["urns"]:
                     if urn in UnExpectedRankings:
                         Result["errors"].append(PrintError("Package contain unexpected URN",  job["id"],
                            "http://one-packaging-api.service.owf-"+namespace.Env+"/v1/package/"+metadata["packagingJobId"],
                            "Urn: "+urn,
                            namespace.Env))
                for RankingUrn in ExpectedRankings:
                     if RankingUrn not in PackageJob["urns"]:
                         Result["errors"].append(PrintError("Missed URN in package",  job["id"],
                            "http://one-packaging-api.service.owf-"+namespace.Env+"/v1/package/"+metadata["packagingJobId"],
                            "Urn: "+RankingUrn,
                            namespace.Env))
            except:
                Result["errors"].append(PrintError("Failed to get a Job", job["id"],
                                                   "http://one-packaging-api.service.owf-" + namespace.Env + "/v1/package/" +
                                                   metadata["packagingJobId"],
                                                   "None",
                                                   namespace.Env))
        else:
            Result["warn"].append(PrintWarn("There are no PackageURN: "+job["id"]))
    Result["Requests"] = Result["Requests"]+len(Jobs)
    PrettyPrint("Finish Package Checks")
    return Result

def ConductorCheck(Result, namespace):
    PrettyPrint("Start Conductor Checks")
    Jobs = GetInProgressJobsOnOrders(namespace)
    for job in Jobs:
        for ranking in job["rankings"]:
            for workflow in ranking["workflows"]:
                conductorWorkflow = GetWorkflowInfo(workflow["id"], namespace.Env)
                time.sleep(namespace.DelayInMs / 1000)
                if conductorWorkflow["status"] == "FAILED":
                    Result["errors"].append(PrintError("Workflow Failed but job in progress",
                                                       job["id"],
                        "http://conductor-server.service.owf-"+namespace.Env+"/api/workflow/"+workflow["id"]+"?includeTasks=false",
                        "RankingID:"+ranking["id"],
                        namespace.Env))
    Result["Requests"] = Result["Requests"]+len(Jobs)
    PrettyPrint("Finish Conductor Checks")
    return Result

def ValidateReport(Records, Result, filename, Env):
    WorkStatuses = ["IN_PROGRESS","COMPLETED"]
    WorkStages = ["DELIVERED", "DELIVERING"]
    for job in Records:
        if(job["Job Status"] in WorkStatuses
                and job['Workability Date UTC'] == "None"
                and job['Job Type'] == "DELIVERY"
                and job['Job Stage'] != "RESOLVING_MATERIALS"
                and job['Job Stage'] != "LOCALIZATION"):
            Result["errors"].append(PrintError("Workability Date is not set in report",
                                               job["Job GUID"],
                                               "\n\tReport\tFile:\t"+filename+".xlsx"+
                                               "\nReports: "+job["Workability Date UTC"],
                                               "Status: "+job["Job Status"],
                                               Env))
        if(job["Job Status"] == "COMPLETED"
                and job['Ship Date UTC'] == "None"
                and job['Job Type'] == "DELIVERY"
                and job['Job Stage'] in WorkStages):
            Result["errors"].append(PrintError("Ship Date is not set in report",
                                               job["Job GUID"],
                                               "\n\tReport\tFile:\t"+filename+".xlsx"+
                                               "\nReports: "+job["Ship Date UTC"],
                                               "Status: "+job["Job Status"],
                                               Env))
    return Result

def GetAllJobs(StartDate, EndDate, Env, Delay):
    Result = []
    payload = '{"searchConditions":[' \
              '{"propertyName":"createdAt","operator":"gt","value":"'+\
              StartDate+'",' \
              '"nextConditionOperator":"And"},' \
              '{"propertyName":"createdAt","operator":"lt","value":"'+\
              EndDate+'",' \
              '"nextConditionOperator":"And"}],' \
              '"sortBy":"orderHrId",' \
              '"sortDirection":"Desc",' \
              '"offset":1,' \
              '"limit":100,' \
              '"includeProgress":false,' \
              '"includeBillingInfo":false,' \
              '"includeRankings":false,' \
              '"includeRankingsWorkflows":false,' \
              '"includeRankingsProgress":false,' \
              '"includeRankingsLanguages":false,' \
              '"includeRankingsLinks":false,' \
              '"includeLinks":false,' \
              '"includeAttachments":false,' \
              '"includeRankingOverrides":false,' \
              '"includeQcSignatures":false,' \
              '"includeDeliverableComponents":false}'
    while True:
        DeliveryJobs = SearchDeliveryJobs(payload, Env)
        Result = Result + DeliveryJobs
        data = json.loads(payload)
        data["offset"] = data["offset"] + 100
        payload = json.dumps(data)
        print("*", end="|")
        time.sleep(Delay)
        if len(DeliveryJobs) != 100:
            print("*")
            break
    return Result

def CheckReportStatuses(Records, Jobs, Result, ReportDate, filename, Env):
    for OrderJob in Jobs:
        for ReportJob in Records:
            if ReportJob["Job GUID"] == OrderJob["id"]:
                if ReportJob["Job Status"] != OrderJob["status"]:
                    d2 = datetime.strptime(ReportDate,"%Y-%m-%dT%H:%M:%S.%f")
                    d1 = datetime.strptime(OrderJob["lastModifiedAt"],"%Y-%m-%dT%H:%M:%S.%f")
                    Interval = d2 - d1
                    Result["errors"].append(PrintError("Status Mismatch",
                                                       ReportJob["Job GUID"],
                                                       "\n\tReport\tFile:\t" + filename + ".xlsx" +
                                                       "\n\tReport\tStatus:\t" + ReportJob["Job Status"]+
                                                       "\n\tJob\tStatus:\t"+ OrderJob["status"],
                                                       "\n\tReport\t\tDate:\t" +ReportDate+
                                                       "\n\tJob Update\tDate:\t"+ OrderJob["lastModifiedAt"]+
                                                       "\n\tTime\tInterval:\t"+str(Interval),
                                                       Env))
                break;
    return Result

def ReportCheckPart(Result, StartDate, EndDate, filename, namespace):
    print("Time Interval: "+StartDate+" => "+EndDate)
    print("Get Jobs")
    Jobs = GetAllJobs(StartDate, EndDate, namespace.Env, namespace.DelayInMs)
    print("Retreived: "+str(len(Jobs)))
    Result["Requests"] = Result["Requests"] + math.ceil(len(Jobs)/100)
    print("Download Report")
    DownloadReport(StartDate, EndDate, filename, namespace.Env)
    print("Convert to csv")
    Records = ConvertToCsv(filename)
    ReportDate = datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%f")
    ##print(len(Records))
    print("Check Report data")
    Result = ValidateReport(Records, Result, filename, namespace.Env)
    print("Check Statuses")
    Result = CheckReportStatuses(Records, Jobs, Result, ReportDate, filename, namespace.Env)
    return Result

def ReportCheck (Result, namespace):
    PrettyPrint("Start Report Checks")
    step = 14
    for interval in range(step, namespace.Days, step):
        filename = "Report_"+str(interval)
        StartDate = (datetime.today() - timedelta(days=interval)).strftime("%Y-%m-%dT%H:%M:%S")
        EndDate = (datetime.today() - timedelta(days=interval-step)).strftime("%Y-%m-%dT%H:%M:%S.%f")
        Result = ReportCheckPart(Result, StartDate, EndDate, filename, namespace)
    if(namespace.Days % step != 0):
        filename = "Report_latest"
        interval = namespace.Days % step
        StartDate = (datetime.today() - timedelta(days=namespace.Days)).strftime("%Y-%m-%dT%H:%M:%S")
        EndDate = (datetime.today() - timedelta(days=namespace.Days-interval)).strftime("%Y-%m-%dT%H:%M:%S.%f")
        Result = ReportCheckPart(Result, StartDate, EndDate, filename, namespace)
    if(namespace.Days == 14):
        filename = "Report_latest"
        StartDate = (datetime.today() - timedelta(days=namespace.Days)).strftime("%Y-%m-%dT%H:%M:%S")
        EndDate = (datetime.today()).strftime("%Y-%m-%dT%H:%M:%S.%f")
        Result = ReportCheckPart(Result, StartDate, EndDate, filename, namespace)
    PrettyPrint("Finish Report Checks")
    return Result

PrettyPrint("Init Parameters")
parser = createParser()
namespace = parser.parse_args(sys.argv[1:])
print(namespace)
PrettyPrint("Check Mode is "+namespace.Check)
Result = {"errors": [] , "warn":[], "Requests":0}
start_time = time.time()
if(namespace.Check == "Reports" or namespace.Check == "All"):
    Result = ReportCheck(Result, namespace)
    print(Result["Requests"])
    print("--- %s seconds ---" % (time.time() - start_time))
if(namespace.Check == "Sherlock" or namespace.Check == "All"):
    Result = SherlockCheck(Result, namespace)
    print(Result["Requests"])
    print("--- %s seconds ---" % (time.time() - start_time))
if(namespace.Check == "Delivery" or namespace.Check == "All"):
    Result = DeliveryCheck(Result, namespace)
    print(Result["Requests"])
    print("--- %s seconds ---" % (time.time() - start_time))
if(namespace.Check == "Package" or namespace.Check == "All"):
    Result = PackageCheck(Result, namespace)
    print(Result["Requests"])
    print("--- %s seconds ---" % (time.time() - start_time))
if(namespace.Check == "Conductor" or namespace.Check == "All"):
    Result = ConductorCheck(Result, namespace)
    print(Result["Requests"])
    print("--- %s seconds ---" % (time.time() - start_time))
#print(Result)
for warn in (Result["warn"]):
    print(warn)
print("Warns:"+str(len(Result["warn"])))
print("errors:"+str(len(Result["errors"])))
print("total requests:: "+str(Result["Requests"]))
print("Save results")
with open("log.txt", 'w') as f:
    for error in Result["errors"]:
        f.write(error)
    for warn in Result["warn"]:
        f.write(warn)
    f.close()
with open("Result.txt", 'w') as f2:
    data = "\\n>_Warns_\\t:\\t" + str(len(Result["warn"]))
    data = data + "\n\\n>_Errors_\\t:\\t" + str(len(Result["errors"]))
    data = data + "\n\\n>_Total requests_\\t:\\t" + str(Result["Requests"])
    f2.write(data)
PrettyPrint("There are no more checks")

