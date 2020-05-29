from requests.auth import HTTPBasicAuth
from datetime import date, datetime, timedelta
from dateutil.parser import parse
from random import random
import execute_time as ex
import pandas as pd
import requests
import config
import pyodbc
import time
import json


#### API response handling ####

def fetch_data(url, filename, start_time, ticketid=None, params=None):

    # get API call then check and log the http status code.
    logg_file = open(filename, "a+")
    auth=HTTPBasicAuth(config.jb_username, config.jb_password)
    base_sleep_time = 60
    attempt = 1

    response = requests.get(url, params=params, auth=auth)
    if response.ok:
        logg_file.write(f'INFO - GET {url} returned successful response code: {response.status_code} - RunTime: {start_time}\n')
        return response.json()
    elif response.status_code == 429:
        while response.status_code == 429 and attempt < 4:
            time.sleep(base_sleep_time + (random() * 10))
            response = requests.get(url, params=params, auth=auth)
            logg_file.write(f'WARNING - GET {url} returned unexpected response code: {response.status_code} - Attempt {attempt} - RunTime: {start_time}\n')
            attempt+=1
        if response.status_code == 429:
            logg_file.write(f'CRITICAL - Final GET {url} returned unexpected response code: {response.status_code} - RunTime: {start_time}\n')
        else:
            return response.json()
    else:
        logg_file.write(f'CRITICAL - GET {url} returned unexpected response code: {response.status_code} - RunTime: {start_time}\n')

#### Grab list of tickets based on  last time script was run ####

def get_tickets(start_time,filename):
    
    # last_execute is the timestamp from the last time the script was run
    last_executed = str(pd.to_datetime(ex.execute_time) - timedelta(hours=0, minutes=60))
    print(last_executed)
    
    jb_param = {
        'offset':'1',
        'count':'100',
        'updatedFrom':last_executed
    }

    ticketlist = fetch_data("https://"+config.jb_url+"/helpdesk/api/Tickets?", filename, start_time, None, jb_param)
    id_list = []
    i = 0
    for i in range(len(ticketlist)):
        ticketid = ticketlist[i]['IssueID']
        id_list.append(ticketid)
        i+=1
    return id_list

### Parses out the normal field API response ###

def get_fields(tix_response):
    try:
        # get normal ticket fields from API response
        ticketid = str(tix_response['TicketID'])
        createdate = str(tix_response['IssueDate'])
        subject = str(tix_response['Subject'])
        status = str(tix_response['Status'])
        custusername = str(tix_response['SubmitterUserInfo']['FullName'])
        duedate = str(tix_response['DueDate'])
        lastupdate = str(tix_response['LastUpdated']) #if status != "New" else str(tix_response['LastUpdated'] - datetime.timedelta(seconds=1))
        custdept = str(tix_response['SubmitterUserInfo']['DepartmentName'])
        tags = []
        tag_list = [tags.append(tix_response['Tags'][i]['Name']) for i in range(len(tix_response['Tags'])) if len(tix_response['Tags'][i]) > 0]
        try:
            tag = ";".join(tags)
        except TypeError:
            tag = None
        try:
            assignedto = str(tix_response['AssigneeUserInfo']['FullName'])
        except TypeError:
            assignedto = None
        try:
            assignedtodept = str(tix_response['AssigneeUserInfo']['DepartmentName'])
        except TypeError:
            assignedtodept = None
        resolvedate = str(tix_response['ResolvedDate'])
        categoryname = str(tix_response['CategoryName'])
        try:
            category, detail, = categoryname.split("/", 1)
        except ValueError:
            category, detail = categoryname, None

        # Parse date variables to make updated variable a date data type

        createdate_1 = parse(createdate, fuzzy=True)
        updatedate = parse(lastupdate, fuzzy=True)
        try:
            resolvedate_1 = parse(resolvedate, fuzzy=False, default= None)
        except ValueError:
            resolvedate_1 = None
        try:
            duedate_1 = parse(duedate, fuzzy=False, default= None)
        except ValueError:
            duedate_1 = None
        
        
        nf_dict = {
            "ticketid":ticketid, "assignedtodept":assignedtodept, "assignedto":assignedto, "createdate":createdate, "status":status,
            "duedate_1":duedate_1,	"resolvedate_1": resolvedate_1,	"custdept":custdept,	"custusername":custusername, "category":category,
            "detail":detail,	"subject":subject, "lastupdate":lastupdate, "tag":tag
                }
        return nf_dict
    except Exception as e:
        return ("Failed to get normal field for TicketId: {0} : Error:{1} \n".format( str(ticketid),str(e)))

### Parses out the custom field API response ###
                    
def get_customfields(ticketid,customfield_response):
    try:
        
        arch_priority = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Archer Priority']
        casename = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Case Name']
        agency_col = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Agency/Collector Associated with Task']
        req_size = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Request Size']
        arch_status = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Archer Status']
        linked_id = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Linked Ticket Number']
        ticket_diff = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Ticket Difficulty']
        process_time = [customfield_response[i]['Value'] for i in range(0, len(customfield_response)) if customfield_response[i].get('FieldName', None) == 'Processing Time']

    # Assign data type for custom fields
        
        try:
            arch_priority = str(arch_priority[0]).strip()
        except IndexError:
            arch_priority = None
        try:
            casename = str(casename[0]).strip()
        except IndexError:
            casename = None
        try:
            agency_col = str(agency_col[0]).strip()
        except IndexError:
            agency_col = None
        try:
            req_size = str(req_size[0]).strip()
        except IndexError:
            req_size = None
        try:
            arch_status = str(arch_status[0]).strip()
        except IndexError:
            arch_status = None
        try:
            linked_id = str(linked_id[0]).strip()
        except IndexError:
            linked_id = None
        try:
            ticket_diff = str(ticket_diff[0]).strip()
        except IndexError:
            ticket_diff = None
        try:
            process_time = str(process_time[0]).strip()
        except IndexError:
            process_time = None

        cf_dict = {
            "ticketid": ticketid, "arch_priority":arch_priority, "casename": casename, "agency_col":agency_col, "req_size":req_size,
                "arch_status":arch_status, "linked_id":linked_id, "ticket_diff":ticket_diff, "process_time":process_time
                }
        return cf_dict
    except Exception as e:
        return ("Failed to get custom field for TicketId: {0} : Error:{1} \n".format( str(id),str(e)))

def update_sql(ticketid, nf_dict, cf_dict):
    try:
        connection = pyodbc.connect(
        r'DRIVER={SQL Server Native Client 11.0};'
        r'SERVER=' + config.server + ';'
        r'DATABASE=' + config.database + ';'
        r'UID=' + config.username + ';'
        r'PWD=' + config.password
        )

        cursor = connection.cursor()
        
        # Create JitBit backup table
        jitbit_backup = 'DROP TABLE IF EXISTS JitBit_BackUp; SELECT * INTO JitBit_BackUp FROM JitBit'
        cursor.execute(jitbit_backup)

        # Update ticket in SQL
        if str(ticketid) == nf_dict['ticketid']:
            sql_insert = ('INSERT INTO JitBit (TicketId, TechTeam,	Technician,	CreateDate,	Status,	DueDate,	ResolveDate,	RequesterTeam,	Requester,	Casename,	Agency_Collector,	Category,	Detail,	Subject,	DifficultyLevel,	ProcessTime,	Archer_Priority,	LinkedTicket, UpdateDate, Tags) values (?, ?, ?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?, ?, ?)')
            cursor.execute(sql_insert, (nf_dict['ticketid'],	nf_dict['assignedtodept'],	nf_dict['assignedto'],	nf_dict['createdate'],	nf_dict['status'],	nf_dict['duedate_1'],	nf_dict['resolvedate_1'],	nf_dict['custdept'],	nf_dict['custusername'],	cf_dict['casename'],	cf_dict['agency_col'],	nf_dict['category'], nf_dict['detail'],	nf_dict['subject'],	cf_dict['ticket_diff'],	cf_dict['process_time'],	cf_dict['arch_priority'],	cf_dict['linked_id'], nf_dict['lastupdate'], nf_dict['tag']))
        else:
            sql_update = ( "UPDATE JitBit SET Status = 'Merged', LinkedTicket = ? WHERE TicketId = ? AND UpdateDate = (SELECT MAX(UpdateDate) FROM JitBit WHERE TicketId = ?)")
            cursor.execute(sql_update, (nf_dict['ticketid'], ticketid, ticketid))
        sql_dedup = ('exec JitBit_DeDup')
        cursor.execute(sql_dedup)
        cursor.commit()
        
        print ("Done. JitBit Updated for {0}".format(str(nf_dict['ticketid'])))
    except Exception as e:
        print ("TicketId: {0} : Error:{1} \n".format( str(ticketid),str(e)))


def main():

    # Creates a log file based on the date its run.
    # Start_time creates a timestamp of when the script was executed. 
    # It will be written in a file called execute.py at the end of this script

    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = date.today()
    today_file = "log_file\\logfile-{0}.txt".format(str(today))
    logg_file = open(today_file, "a+")

    id_list = get_tickets(start_time, today_file)

    for id in id_list:
        try:
            # make API requests with a 1 sec wait between each request
            r_normfields  = fetch_data("https://"+ config.jb_url +"/helpdesk/api/Ticket?id="+str(id), today_file, start_time, str(id))
            time.sleep(1)
            r_custfields = fetch_data("https://"+config.jb_url+"/helpdesk/api/TicketCustomFields?id="+str(id),today_file, start_time, str(id))
            time.sleep(1)

            # parse response 
            ticket_fields = get_fields(r_normfields)
            ticket_custfields = get_customfields(id,r_custfields)

            # update SQL
            if isinstance(ticket_fields, dict) and isinstance(ticket_custfields, dict):
                update_sql(id, ticket_fields, ticket_custfields)
            else: 
                logg_file.write("CRITICAL - DictionaryCheck is False.\n {0} - {1} \n".format(ticket_fields, ticket_custfields))
                print("DictionaryCheck is False.\n" + ticket_fields +" - "+ ticket_custfields)
                pass
        except Exception as e:
            logg_file.write("CRITICAL - Code broke for ticketid {0}. Error: {1}\n".format(str(id),str(e)))
            print("Code broke for ticketid {0}. Error: {1}".format(str(id),str(e)))
    
    f = open('execute_time.py', 'w+')
    # log the start time of this script to a seperate file which will be called on next time the script is run.
    f.write('execute_time =' + '"' + start_time + '"')

main()
            