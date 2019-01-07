# DistilToolbox v0.5.4
# Created by Emanuele Bonura Oct 2018
# Purpose of this is to have a collection of utilities and scripts in one place
# Everything has to run from Jupyter lab and minimise usage of additional softwares
# Base for this project was the quickly workbooks
# This got expanded with interactive graphs and additional utilities

# Need quite a bunch of import
from impala.dbapi import connect as _connect
from impala.util import as_pandas as _as_pandas
from getpass import getpass as _getpass
from configparser import ConfigParser as _ConfigParser
from os import path as _path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
from hashlib import md5
import qgrid
from pivottablejs import pivot_ui
from plotly.offline import init_notebook_mode, plot, iplot
import cufflinks as cf
import ipywidgets as widgets

# Plotly + Cufflinks initialization
init_notebook_mode(connected=True)
cf.set_config_file(offline=False, world_readable=True, theme='ggplot')
cf.go_offline()


# Main Class
class DistilToolbox():

    def __init__(self, query=None, where=None):
        plt.style.use('ggplot')
        self.where = where
        self.queries = {}
        # Save all queries I possibly need
        if query is not None:
            self.queries['query_user'] = query
        # Here starts a long dictionary of pre-determined queries
        self.queries['query_domains'] = '''SELECT * FROM prd.account_domains'''
        self.queries['query_err'] = '''SELECT  CAST(floor(CAST(access_time AS bigint) / (60))*(60) AS TIMESTAMP) 'access_time'
                                                ,sum( IF(http_status_code = 502 AND origin_http_status_code = '-', 1, 0 ) ) '502_upstream_fail'
                                                ,sum( IF(http_status_code = 502 AND origin_response_time != '-' AND floor(CAST(origin_response_time AS double)) < 5 , 1, 0 ) ) '502_upstream_disconnect'
                                                ,sum( IF(http_status_code = 504 AND floor(CAST(origin_response_time AS double)) < 16 , 1, 0 ) ) '504_connect_timeout'
                                                ,sum( IF(http_status_code = 504 AND floor(CAST(origin_response_time AS double)) > 15 , 1, 0 ) ) '504_receive_timeout'
                                                ,sum( IF(http_status_code = 503, 1, 0) ) '503_service_unavailable'
                                                ,sum( IF(http_status_code = 499, 1, 0) ) '499_client_disconnect'
                                                ,sum( IF(http_status_code > 499 AND http_status_code not in (502,503,504), 1, 0) ) '50x_origin_error'
                                        FROM    prd.web_logs
                                        WHERE   {where}
                                                AND is_billable = true
                                        GROUP BY access_time'''
        self.queries['query_category'] = '''SELECT  cast(floor(cast(access_time as bigint)) AS timestamp) 'access_time'
                                                    ,sum( `_is_human` ) 'humans'
                                                    ,sum( `_is_good_bot` ) 'good_bots'
                                                    ,sum( `_is_whitelist` ) 'whitelisted'
                                                    ,sum( if(`_is_bad_bot` > 0 and distil_action in ('@proxy','@proxy_ajax','@proxy_inject'), 1, 0 )) 'proxied_bad_bots'
                                                    ,sum( if( `_is_bad_bot` > 0 and distil_action not in ('@proxy','@proxy_ajax','@proxy_inject'), 1, 0 )) 'mitigated_bad_bots'
                                            FROM    prd.web_logs
                                            WHERE   {where}
                                                    AND is_billable = true
                                            GROUP BY access_time'''
        self.queries['query_general'] = '''SELECT access_time, day, month, ip, request_url, geo_ip_organization,
                                                violations, distil_action, user_agent, http_status_code, origin_http_status_code, id_provider, path_type,
                                                pages_per_session, session_length_seconds, hsig, primitive_id, informed_id, zid, zuid, account_id, domain_id
                                                , _request_url_path, js_additional_threats, js_known_violators_additional_threats, valid_ajax
                                                ,_is_human , _is_good_bot, _is_whitelist, _is_bad_bot, http_referrer
                                            FROM    prd.web_logs
                                            WHERE   {where}
                                            AND is_billable = true'''
        self.queries['query_all'] = '''SELECT *
                                        FROM    prd.web_logs
                                        WHERE   {where}
                                        AND is_billable = true'''                                       
        self.queries['query_general_count'] = '''SELECT COUNT(access_time)
                                                FROM    prd.web_logs
                                                WHERE   {where}
                                                    AND is_billable = true'''
        self.queries['query_hours'] = '''SELECT account_id, domain_id, year, month, day
                                            ,hour
                                            ,count(*) 'count'
                                        FROM cs_mrogers.smart_logs
                                        WHERE {where}
                                        GROUP BY account_id
                                                ,domain_id
                                                ,year
                                                ,month
                                                ,day
                                                ,hour
                                        ORDER BY 1,2,3,4,5,6'''
        self.queries['query_smarter'] = '''SELECT *
                                        FROM   cs_mrogers.smart_logs
                                        WHERE  {where}
                                        ORDER BY access_time asc
                                        '''
        # Preliminary query to check the count 
        self.queries['query_sliced_count'] ='''SELECT {sliced_by}
                                                ,COUNT({sliced_by})
                                        FROM    prd.web_logs
                                        WHERE   {where}
                                                AND is_billable = true
                                        GROUP BY {sliced_by}
                                        ORDER BY COUNT({sliced_by}) DESC'''
        
        self.queries['query_sliced'] = '''SELECT 
                                                CAST(floor(CAST(access_time AS bigint) / (60))*(60) AS TIMESTAMP) 'access_time'
                                                ,COUNT(access_time)
                                                ,{sliced_by}
                                        FROM    prd.web_logs
                                        WHERE   {where}
                                                AND is_billable = true
                                        GROUP BY access_time
                                                ,{sliced_by}''' 
        self.queries['query_sliced_remaining'] = '''SELECT 
                                                CAST(floor(CAST(access_time AS bigint) / (60))*(60) AS TIMESTAMP) 'access_time'
                                                ,COUNT(access_time)
                                                ,'Remaining' AS {sliced_by}
                                        FROM    prd.web_logs
                                        WHERE   {where}
                                                AND is_billable = true
                                        GROUP BY access_time
                                                ,{sliced_by}''' 
        self.queries['query_investigate']='''SELECT
                                                cast(floor(cast(access_time as bigint)) AS timestamp) 'access_time'
                                                , sum(`_is_human`) 'humans'
                                                , sum(`_is_good_bot`) 'good_bots'
                                                , sum(`_is_whitelist`) 'whitelisted'
                                                , sum(if(`_is_bad_bot` > 0 and distil_action in ('@proxy','@proxy_ajax','@proxy_inject'), 1, 0 )) 'proxied_bad_bots'
                                                , sum(if(`_is_bad_bot` > 0 and distil_action not in ('@proxy','@proxy_ajax','@proxy_inject'), 1, 0 )) 'mitigated_bad_bots'
                    
                                                , ndv(ip) 'distinct_ips'
                                                , ndv(hsig) 'distinct_hsigs'
                                                , ndv(primitive_id) 'distinct_primitiveIDs'
                                                , ndv(zid) 'distinct_ZIDs'
                                                , ndv(zuid) 'distinct_ZUIDs'
                                                , ndv(geo_ip_organization) 'distinct_geo_ip_org'
                                                , ndv(user_agent) 'distinct_UAs'

                                                , sum(if(zuid = '', 1, 0))          'no_fingerprint'
                                                , sum(if(http_referrer = '', 1, 0)) 'no_referrer'

                                                , sum(_is_monitored)        'Monitored_Y' 
                                                , sum(_is_captcha_served)   'Captcha_Y' 
                                                , sum(_is_blocked)          'Blocked_Y' 
                                                , sum(_is_dropped)          'Dropped_Y'

                                                , sum(_is_captcha_served)   'captcha_served'
                                                , sum(_is_captcha_failed)   'captcha_failed' 
                                                , sum(_is_captcha_attempted) 'captcha_attempted'
                                                , sum(_is_captcha_solved)   'captcha_solved'

                                                , sum(if(whitelist > 0 and whitelist & 2 > 0                                    , 1, 0)) 'WL_Country_ACL'
                                                , sum(if(whitelist > 0 and whitelist & 8 > 0                                    , 1, 0)) 'WL_Search_Engine_ACL'
                                                , sum(if(whitelist > 0 and whitelist & 16 > 0                                   , 1, 0)) 'WL_Social_Media_ACL'
                                                , sum(if(whitelist > 0 and whitelist & 32 > 0                                   , 1, 0)) 'WL_Internal_Request'
                                                , sum(if(whitelist > 0 and whitelist & 64 > 0                                   , 1, 0)) 'WL_Referrer_ACL'
                                                , sum(if(whitelist > 0 and whitelist & 2048 > 0                                 , 1, 0)) 'WL_IP_ACL'
                                                , sum(if(whitelist > 0 and whitelist & 1048576 > 0                              , 1, 0)) 'WL_GeoIP_Org_ACL'
                                                , sum(if(whitelist > 0 and whitelist & 4194304 > 0                              , 1, 0)) 'WL_User_Agent_ACL'
                                                , sum(if(whitelist > 0 and whitelist & 8388608 > 0                              , 1, 0)) 'WL_Unique_Identifier_ACL'
                                                , sum(if(whitelist > 0 and whitelist & 16777216 > 0                             , 1, 0)) 'WL_Header_ACL'
                                                , sum(if(whitelist > 0 and whitelist & 134217728 > 0                            , 1, 0)) 'WL_Static_Extension'

                                                , sum(if(path_type = 'web' and violations > 0 and violations & 16 > 0			, 1, 0)) 'Web_PPM'
                                                , sum(if(path_type = 'web' and violations > 0 and violations & 128 > 0			, 1, 0)) 'Web_Session_Length'
                                                , sum(if(path_type = 'web' and violations > 0 and violations & 256 > 0			, 1, 0)) 'Web_PPS'
                                                , sum(if(path_type = 'web' and violations > 0 and violations & 516 > 0			, 1, 0)) 'Web_Identities'
                                                , sum(if(path_type = 'web' and violations > 0 and violations & 1024 > 0			, 1, 0)) 'Web_Aggregator_UA'
                                                , sum(if(path_type = 'web' and violations > 0 and violations & 12288 > 0		, 1, 0)) 'Web_Automated_Browsers'
                                                , sum(if(path_type = 'web' and violations > 0 and violations & 98345 > 0	    , 1, 0)) 'Web_Known_Violators' 
                                                , sum(if(path_type = 'web' and violations > 0 and violations & 131072 > 0		, 1, 0)) 'Web_Cookie_Tampering'
                                                , sum(if(path_type = 'web' and violations > 0 and violations & 2097152 > 0		, 1, 0)) 'Web_KVDC'
                                                , sum(if(path_type = 'web' and violations > 0 and violations & 100663296 > 0 	, 1, 0)) 'Web_Other'

                                                , sum(if(violations > 0 and violations & 2 > 0			                       	, 1, 0)) 'uACL_Country_Block'
                                                , sum(if(violations > 0 and violations & 64 > 0			                    	, 1, 0)) 'uACL_Referrer_Block'
                                                , sum(if(violations > 0 and violations & 2048 > 0		                    	, 1, 0)) 'uACL_IP_Block'
                                                , sum(if(violations > 0 and violations & 16384 > 0			                    , 1, 0)) 'Web_Machine_Learning'
                                                , sum(if(violations > 0 and violations & 1048576 > 0		                    , 1, 0)) 'uACL_Org_Block'
                                                , sum(if(violations > 0 and violations & 4194304 > 0		                    , 1, 0)) 'uACL_UA_Block'
                                                , sum(if(violations > 0 and violations & 8388608 > 0		                    , 1, 0)) 'uACL_Unique_ID_Block'
                                                , sum(if(violations > 0 and violations & 16777216 > 0  		                    , 1, 0)) 'uACL_Header_Block'
                                                , sum(if(violations > 0 and violations & 134217728 > 0		                    , 1, 0)) 'uACL_Extension_Block'

                                                --, sum(if(path_type = 'api' and violations > 0 and violations & 516 > 0			, 1, 0)) 'webAPI_Identities'
                                                --, sum(if(path_type = 'api' and violations > 0 and violations & 1024 > 0			, 1, 0)) 'webAPI_Aggregator_UA'
                                                --, sum(if(path_type = 'api' and violations > 0 and violations & 12288 > 0	    , 1, 0)) 'webAPI_Automated_Browsers'
                                                --, sum(if(path_type = 'api' and violations > 0 and violations & 229417 > 0		, 1, 0)) 'webAPI_Known_Violators'
                                                --, sum(if(path_type = 'api' and violations > 0 and violations & 2097152 > 0		, 1, 0)) 'webAPI_KVDC'
                                                --, sum(if(path_type = 'api' and violations > 0 and violations & 268435456 > 0    , 1, 0)) 'webAPI_Missing_Unique_ID'
                                                --, sum(if(path_type = 'api' and violations > 0 and violations & 536870912 > 0    , 1, 0)) 'webAPI_Path_PPM'
                                                --, sum(if(path_type = 'api' and violations > 0 and violations & 1073741824 > 0   , 1, 0)) 'webAPI_Path_PPS'

                                                --, sum(if(id_provider = 'sdk' and violations > 0 and violations & 1 > 0          , 1, 0)) 'SDK_Known_Violators' 
                                                --, sum(if(id_provider = 'sdk' and violations > 0 and violations & 128 > 0        , 1, 0)) 'SDK_Path_SL' 
                                                --, sum(if(id_provider = 'sdk' and violations > 0 and violations & 32768 > 0		, 1, 0)) 'SDK_Bad_Client'
                                                --, sum(if(id_provider = 'sdk' and violations > 0 and violations & 131072 > 0		, 1, 0)) 'SDK_Invalid_or_Expired_Token'
                                                --, sum(if(id_provider = 'sdk' and violations > 0 and violations & 524288 > 0		, 1, 0)) 'SDK_Failed_Challenge'
                                                --, sum(if(id_provider = 'sdk' and violations > 0 and violations & 2097152 > 0    , 1, 0)) 'SDK_KVDC' 
                                                --, sum(if(id_provider = 'sdk' and violations > 0 and violations & 268435456 > 0	, 1, 0)) 'SDK_Missing_Unique_ID'
                                                --, sum(if(id_provider = 'sdk' and violations > 0 and violations & 536870912 > 0	, 1, 0)) 'SDK_Path_PPM'
                                                --, sum(if(id_provider = 'sdk' and violations > 0 and violations & 1073741824 > 0	, 1, 0)) 'SDK_Path_PPS'
                                            FROM    prd.web_logs
                                            WHERE   {where}
                                                    AND is_billable = true
                                                    AND allowed != 22
                                            GROUP BY    access_time'''
        self.queries['query_top_paths']='''SELECT 	COUNT(_request_url_path)
                                                    ,_request_url_path
                                            FROM 	prd.web_logs
                                            WHERE 	{where}
                                                    AND allowed != 22
                                            GROUP BY _request_url_path'''
        self.queries['query_additional_threats']='''SELECT  cast(floor(cast(access_time as bigint)) AS timestamp) 'access_time'
                                                    ,COUNT(js_additional_threats) 'js_add_threats_count'
                                                    ,js_additional_threats
                                            FROM    prd.uuid_logs
                                            WHERE   {where}
                                                    AND is_billable = true
                                            GROUP BY access_time
                                                    ,js_additional_threats'''

        # Query to retrieve columns in the database
        df_desc = self.impala_connect(query = '''DESCRIBE prd.web_logs''', show_query = False)
        self.table_fields = dict(df_desc.iloc[:,0])


    def impala_connect(self, query = None, force_reconnect = False, show_query = True):
        if query is None:
            self.queries['query_user']
        '''Connect to impala and retrieves query result, returns pandas DataFrame'''
        # Creates an hash of the query and check if a file with this name exist,
        # if I run it already I don't want to do it again
        # I also remove all newline, tabs and double spaces
        new_clause = " ".join(str(query).split()).encode('utf-8')
        hash_query = md5(new_clause).hexdigest()

        # Display the query in a widget already collapsed
        if show_query:
            text_area = widgets.Textarea(value = query, layout = {'width': '1000px', 'height': '200px'})
            accordion = widgets.Accordion(children = [text_area], _titles = {'0': 'Query executed:'}, layout = {'margin':'5px'}, selected_index = None)
            display(accordion)

        # Check if hash exists
        if _path.isfile('.' + hash_query) and force_reconnect == False and show_query == True:
            print('Query already stored offline, retrieved from cache: ', hash_query) if show_query else None
            return pd.read_pickle('.' + hash_query)
        else:
            config = _ConfigParser()
            config.read(_path.expanduser('~/.dpcfg.ini'))
            config.get('ldap','password')
            config.get('ldap','username')
            try:
                connection = _connect(host='192.225.214.212'
                                    ,port=21050
                                    ,auth_mechanism='PLAIN'
                                    ,user=config.get('ldap','username')
                                    ,password=config.get('ldap','password'))
            except:
                print('Connection returned error, are you connected to the VPN?:', sys.exc_info()) if show_query else None
                return
            cursor = connection.cursor()
            try:
                cursor.execute(query)
                return_df = _as_pandas(cursor)
                return_df.to_pickle('.' + hash_query)
                return return_df
            except:
                print('Query returned error:', sys.exc_info()) if show_query else None
    
    def qgrid(self, df, grid_options = {'forceFitColumns': False, 'defaultColumnWidth': 150}):
        '''Simple wrapper for the qgrid function, when provided a dataframe, it will return the interactive widget.
        You can specify the options in grid_options passing them as dictionary'''
        self.qgrid_widget = qgrid.show_grid(df, grid_options=grid_options)
        print("You can access the filtered DataFrame with .qgrid_widget.get_changed_df()")
        return self.qgrid_widget
    
    def pivot_ui(self, df):
        '''Simple wrapper for the pivot_ui function'''
        return pivot_ui(df)
    
    def set_index_first_col(self, df = None, dt_convert = True, c_num = 0):
        '''Set the index to the first column of the dataframe, in convert=True it will also convert to datetime, 
        in case c_num is set to a different value it will chose another column instead of the first one'''
        df.set_index(df.columns[c_num], inplace = True)
        if dt_convert:
            df.index = pd.to_datetime(df.index, unit = 's')
    
    def violation_decoder_web(self, code):
        '''Equivalent to vcode script in ruby, translates violation code to explanation'''
        codes = {-1: "Unchecked, we don't check the request",
                0: "No violation detected",
                1: "Known Violators",
                2: "Country Block",
                4: "Browser Integrity Check",
                8: "Known Violator User Agent",
                16: "Pages Per Minute Exceeded",
                32: "Known Violator Honeypot",
                64: "Referrer Block",
                128: "Session Length Exceeded",
                256: "Pages Per Session Exceeded",
                512: "Bad User Agent",
                1024: "Aggregator User Agents",
                2048: "IP",
                4096: "JavaScript Not Loaded",
                8192: "JavaScript AJAX Not Completed",
                16384: "Mentat",
                32768: "Known Violator Automation Tool",
                65536: "Form Spam Submission",
                131072: "Cookie Tampering",
                262144: "IP Pinning Threat",
                524288: "Invalid JS Test Results",
                1048576: "GeoIP Org ACL",
                2097152: "Known Violator Data Center",
                4194304: "User Agent ACL",
                8388608: "Unique Identifier ACL",
                16777216: "Header Name:Value ACL",
                33554432: "Exceeds Invalid Request Counter",
                67108864: "Maximum CAPTCHA Attempts Exceeded",
                134217728: "Extension ACL",
                268435456: "Missing Unique ID",
                536870912: "Requests Per Minute",
                1073741824: "Requests Per Session"}
        try:
            binary_form = bin(code)[2:]
            output = []
            for place, val in enumerate(reversed(binary_form), 0):
                if val is '1':
                    output.append("{} ({})".format(codes[2**place], 2**place))
            return output
        except Exception as e:
            return ("Could Not Parse Violation Code\n{}".format(e))

    # ------------ #
    # Quickly logs #
    # ------------ #
    def get_hours(self, where = None, force_reconnect = False):
        '''Find the right hour(s) to query'''
        if where is None:
            where=self.where
        self.df_hours = self.impala_connect(self.queries['query_hours'].format(where=where), force_reconnect=force_reconnect)
        self.df_hours.set_index(['year', 'month','day','hour'], inplace=True)
        return self.df_hours

    def show_hours(self):
        try:
            _, ax = plt.subplots(figsize=(20, 10))
            self.df_hours['count'].plot(ax=ax)
            plt.legend(loc='center', bbox_to_anchor=(0.5, 1), ncol=6, fancybox=True, shadow=True)
            plt.show()
        except AttributeError as e:
            print('Error:', e, 'Have you called get_hours() first?')

    def get_smarter(self, where = None, force_reconnect=False):
        '''Get logs for the Excel workbook, formatted for readibility'''
        if where is None:
            where=self.where
        self.df_smarter = self.impala_connect(self.queries['query_smarter'].format(where=where), force_reconnect=force_reconnect)
        self.df_smarter.insert(14, 'utc_time', 0)
        print('Completed, you can export the data using .exportsmarter() or access the dataframe using .df_smarter')

    def export_smarter(self, filename = None):
        '''Get logs for the Excel workbook, formatted for readibility'''
        try:
            if filename is None:
                print('No filename selected')
                return
            print('Exporting to', filename + '.xlsx')
            # Set the variables
            getjs = ["@jst","@pjst"]
            postjs = ["@jst_post"]
            headjs = ["@jst_head"]
            inject = ["@proxy_inject"]
            identify = ["@identify_captcha","@identify_block","@identify_drop","@identify_cookie"]
            mitigate = ["@block","@captcha","@captcha_util","@captcha_correct","@captcha_incorrect",
                        "@captcha_missing_url","@captcha_update_fail","@drop"]
            force = ["@force_identify"]

            # Copy dataframe to excel
            writer = pd.ExcelWriter(filename + '.xlsx',engine='xlsxwriter',options={'strings_to_urls': False})

            # Get row count and column count
            rows = self.df_smarter.shape[0] - 1
            cols = self.df_smarter.shape[1] - 1

            # Convert the dataframe to an XlsxWriter Excel object.
            self.df_smarter.to_excel(writer, index=False)

            # Get the xlsxwriter workbook and worksheet objects.
            wb = writer.book
            ws = writer.sheets['Sheet1']

            for row_num in range(2, rows + 3):
                ws.write_formula(row_num - 1, 14, '=(N%d/86400)+"1/1/1970"' % (row_num))

            # Freeze top row only
            ws.freeze_panes(1, 0)
            ws.autofilter(0, 0, rows, cols)

            class lineFormats:
                def __init__(self, bold, bg_color, font_color):
                    self.bold = bold
                    self.bg_color = bg_color
                    self.font_color = font_color

            jsget = lineFormats(True, '#E05753', '#000000')
            jspost = lineFormats(True, '#6BAB33', '#000000')
            identified = lineFormats(True, '#1886C5', '#000000')
            mitigated = lineFormats(True, '#2A2D30', '#FFFFFF')
            jshead = lineFormats(True, '#E1D07C', '#000000')
            injected = lineFormats(True, '#A3DCFD', '#000000')
            forced = lineFormats(True, '#1886C5', '#FFFFFF')

            jsget_format = wb.add_format(jsget.__dict__)
            jspost_format = wb.add_format(jspost.__dict__)
            identified_format = wb.add_format(identified.__dict__)
            mitigated_format = wb.add_format(mitigated.__dict__)
            jshead_format = wb.add_format(jshead.__dict__)
            injected_format = wb.add_format(injected.__dict__)
            forced_format = wb.add_format(forced.__dict__)

            def getlines(type):
                return self.df_smarter.index[self.df_smarter['distil_action'].isin(type)].tolist()

            # Get rows to format from dataframe
            action_rows = {}
            action_rows['getjs_list'] = getlines(getjs)
            action_rows['postjs_list'] = getlines(postjs)
            action_rows['identify_list'] = getlines(identify)
            action_rows['headjs_list'] = getlines(headjs)
            action_rows['mitigate_list'] = getlines(mitigate)
            action_rows['inject_list'] = getlines(inject)
            action_rows['force_list'] = getlines(force)

            for row in action_rows['getjs_list']:
                ws.set_row(row + 1, None, jsget_format)
            for row in action_rows['postjs_list']:
                ws.set_row(row + 1, None, jspost_format)
            for row in action_rows['identify_list']:
                ws.set_row(row + 1, None, identified_format)
            for row in action_rows['headjs_list']:
                ws.set_row(row + 1, None, jshead_format)
            for row in action_rows['mitigate_list']:
                ws.set_row(row + 1, None, mitigated_format)
            for row in action_rows['inject_list']:
                ws.set_row(row + 1, None, injected_format)
            for row in action_rows['force_list']:
                ws.set_row(row + 1, None, forced_format)

            # Save and close workbook
            writer.save()
            print('Excel file saved')
        except AttributeError as e:
            print('Error:', e, 'Have you called get_smarter() first?')

    # -------------- #
    # Quickly graphs #
    # -------------- #
    def get_errors(self, where = None, force_reconnect=False):
        '''Get a graph displaying errors'''
        if where is None:
            where=self.where

        self.df_error = self.impala_connect(self.queries['query_err'].format(where=where), force_reconnect=force_reconnect)
        self.set_index_first_col(self.df_error)
        print('Completed, you can visualise the data using .show_errors() or access the dataframe using .df_error')
    
    def show_errors(self, group='15T'):
        '''Show the graph displaying errors over time, you can specify time window by 
        nH | hourly frequency 
        nT | minutely frequency 
        nS | secondly frequency
        where n is a number of your choice'''
        try:
            resample_df_err = self.df_error.resample(group).sum()
            layout = dict(title='errors over time',
                        legend=dict(x=0, y=1.5),
                            xaxis=dict(rangeslider=dict(visible = True)))
            resample_df_err.iplot(fill='tozeroy', layout=layout)
        except AttributeError as e:
            print('Error:', e, 'Have you called get_error() first?')

    def get_sliced(self, where = None, sliced_by = None, force_reconnect=False, quantile=.99999):
        '''Get a graph displaying count of request sliced by whatever you want'''
        if where is None:
            where=self.where
            
        # First query to get the top values
        self.df_sliced_count = self.impala_connect(self.queries['query_sliced_count'].format(sliced_by=sliced_by,  where=where), force_reconnect=force_reconnect)
        
        # Select the top values according to the quantile and create an addition to the where clause
        threshold = self.df_sliced_count.quantile(quantile)[-1]
        df = self.df_sliced_count
        filtered_df = df[df.iloc[:,1]>=int(threshold)]
        
        # I need to differentiate depending if the values are int or str
        if type(filtered_df.iloc[:,0].values[0]) == str:
            clause = ','.join("'" + val + "'" for val in filtered_df.iloc[:,0].values)
        else:
            clause = ','.join(str(val) for val in filtered_df.iloc[:,0].values)

        # Connect everything and run
        where_clause = f"{where} AND {sliced_by} IN ({clause})"
        df_slice = self.impala_connect(self.queries['query_sliced'].format(sliced_by = sliced_by,  where=where_clause), force_reconnect=force_reconnect)
        self.set_index_first_col(df_slice)

        # Extract the remaining data and attach
        where_not_clause = f"{where} AND {sliced_by} NOT IN ({clause})"
        try:
            df_slice_remaining = self.impala_connect(self.queries['query_sliced_remaining'].format(sliced_by = sliced_by,  where=where_not_clause), force_reconnect=force_reconnect)
            self.set_index_first_col(df_slice_remaining)
            self.df_sliced = pd.concat([df_slice, df_slice_remaining]).sort_index()
        except:
            pass
        print('Completed, you can visualise the data using .show_sliced() or access the dataframe using .df_sliced')
        
    def show_sliced(self, group='15T'):
        try:
            self.df_sliced['count(access_time)'] = self.df_sliced['count(access_time)'].astype(int) # new line test, need to convert to int in some cases
            table = pd.pivot_table(self.df_sliced, columns=self.df_sliced.columns[-1], values='count(access_time)', index='access_time') 
            resample_get_sliced = table.resample(group).sum()
            layout = dict(legend=dict(x=0, y=1.5), xaxis=dict(tickangle=-45,rangeslider=dict(visible = True)))
            resample_get_sliced.iplot(fill='tozeroy', layout=layout)
        except AttributeError as e:
            print('Error:', e, 'Have you called get_sliced() first?')

    def get_traffic(self, where = None, force_reconnect=False):
        '''Get a graph displaying Web requests'''
        if where is None:
            where=self.where
        try:
            self.df_traffic = self.impala_connect(self.queries['query_category'].format(where=where), force_reconnect=force_reconnect)
            self.set_index_first_col(self.df_traffic)
            print('Completed, you can visualise the data using .show_traffic() or access the dataframe using .df_traffic')
        except:
            print("Could't connect to impala server: ", sys.exc_info())

    def show_traffic(self, group='15T'):
        '''Show the graph displaying the web traffic over time, you can specify time window by 
        nH | hourly frequency 
        nT | minutely frequency 
        nS | secondly frequency
        where n is a number of your choice'''
        try:
            resample_df_traffic = self.df_traffic.resample(group).sum()
            layout = dict(title='web traffic over time',
                legend=dict(x=0, y=1.5)
                ,xaxis=dict(tickangle=-45
                            ,rangeslider=dict(visible = True)))
            resample_df_traffic.iplot(fill='tozeroy', layout=layout)
        except AttributeError as e:
            print('Error:', e, 'Have you called get_traffic() first?')
    
    def show_traffic_perc(self, group='15T'):
        '''Show the graph displaying the web traffic over time, you can specify time window by 
        nH | hourly frequency 
        nT | minutely frequency 
        nS | secondly frequency
        where n is a number of your choice'''
        
        # This function will convert to percentages for me
        def df_to_perc_df(df):
            df_total = df.sum(axis=1)
            df_perc = df.copy()
            for col in df.columns:
                df_perc[col] = df[col]/df_total
            return df_perc
        try:
            resample_df_traffic = self.df_traffic.resample(group).sum()
            layout = dict(title='web traffic over time',
                legend=dict(x=0, y=1.5)
                ,xaxis=dict(tickangle=-45
                            ,rangeslider=dict(visible = True)))
            df_to_perc_df(resample_df_traffic).iplot(fill='tozeroy', layout=layout)
        except AttributeError as e:
            print('Error:', e, 'Have you called get_traffic() first?')
    
    def get_investigate(self, where = None, force_reconnect=False):
        if where is None:
            where=self.where
        self.df_investigate = self.impala_connect(self.queries['query_investigate'].format(where=where), force_reconnect=force_reconnect)
        self.set_index_first_col(self.df_investigate)
        print('Completed, you can visualise the data using .show_investigate() or access the dataframe using .df_investigate')

    def show_investigate(self, group='15T'):
        '''Show the graph displaying a full investigation over time, you can specify time window by 
        nH | hourly frequency 
        nT | minutely frequency 
        nS | secondly frequency
        where n is a number of your choice'''
        try:
            resample_df_investigate = self.df_investigate.resample(group).sum()
            layout = dict(title='web traffic over time',
                legend=dict(x=0, y=1.5),
                xaxis=dict(rangeslider=dict(visible = True)))
            resample_df_investigate[['humans', 'good_bots', 'whitelisted', 'proxied_bad_bots','mitigated_bad_bots']].iplot(fill='tozeroy', layout=layout)
            resample_df_investigate[['distinct_ips', 'distinct_hsigs','distinct_primitiveids', 'distinct_zids', 'distinct_zuids', 'distinct_geo_ip_org','distinct_uas']].iplot(fill='tozeroy', layout=layout)
            resample_df_investigate[['no_fingerprint', 'no_referrer']].iplot(fill='tozeroy', layout=layout)
            resample_df_investigate[['monitored_y', 'captcha_y','blocked_y','dropped_y']].iplot(fill='tozeroy', layout=layout)
            resample_df_investigate[['captcha_served','captcha_failed','captcha_attempted','captcha_solved']].iplot(fill='tozeroy', layout=layout)
            resample_df_investigate[['wl_country_acl','wl_search_engine_acl','wl_social_media_acl','wl_internal_request','wl_referrer_acl','wl_ip_acl','wl_geoip_org_acl','wl_user_agent_acl','wl_unique_identifier_acl','wl_header_acl','wl_static_extension']].iplot(fill='tozeroy', layout=layout)
            resample_df_investigate[['web_ppm','web_session_length','web_pps','web_identities','web_aggregator_ua','web_automated_browsers','web_known_violators','web_cookie_tampering', 'web_kvdc','web_other']].iplot(fill='tozeroy', layout=layout)
            resample_df_investigate[['uacl_country_block','uacl_referrer_block','uacl_ip_block','web_machine_learning','uacl_org_block','uacl_ua_block','uacl_unique_id_block','uacl_header_block','uacl_extension_block']].iplot(fill='tozeroy', layout=layout)
            # resample_df_investigate[['webapi_identities','webapi_aggregator_ua','webapi_automated_browsers','webapi_known_violators','webapi_kvdc','webapi_missing_unique_id','webapi_path_ppm','webapi_path_pps']].iplot(fill='tozeroy', layout=layout)
            # resample_df_investigate[['sdk_known_violators' ,'sdk_path_sl' ,'sdk_bad_client','sdk_invalid_or_expired_token','sdk_failed_challenge','sdk_kvdc' ,'sdk_missing_unique_id','sdk_path_ppm','sdk_path_pps']].iplot(fill='tozeroy', layout=layout)
        except AttributeError as e:
            print('Error:', e, 'Have you called get_investigate() first?')

    # ------------------ #
    # Additional goodies #
    # ------------------ #
    def attach_violation(self, df):
        '''Given a dataframe with a violations field, adds a column with the violation decoded'''
        df['violation_decoded'] = df['violations'].apply(self.violation_decoder_web)

    def get_general(self, where = None, force_reconnect=False, all=False):
        '''Get a very general summary of the account with the most used fields, you can use all=True to get all columns'''
        if where is None:
            where=self.where
        # get the count of rows returned
        count = self.impala_connect(self.queries['query_general_count'].format(where=where)).values[0][0]
        print(count, "rows expected")
        if all:
            self.df_general = self.impala_connect(self.queries['query_all'].format(where=where), force_reconnect=force_reconnect)
        else:
            self.df_general = self.impala_connect(self.queries['query_general'].format(where=where), force_reconnect=force_reconnect)
        self.set_index_first_col(self.df_general)

        # Some manipulation for ease of use
        self.df_general['request_method'], self.df_general['request_path'], self.df_general['request_html'] = self.df_general['request_url'].str.split().str
        self.attach_violation(self.df_general)
        print('Completed, you can access the dataframe using .df_general')


    def get_top_paths(self, where = None, force_reconnect=False):
        '''Get a very general summary of the account'''
        if where is None:
            where=self.where
        self.df_top_paths = self.impala_connect(self.queries['query_top_paths'].format(where=where), force_reconnect=force_reconnect)
        self.df_top_paths.set_index(keys='count(`_request_url_path`)',inplace=True)
        self.df_top_paths.sort_index(ascending=False, inplace=True)
        print('Completed, you can access the dataframe using .df_top_paths')

    def get_top_values(self, df, n=None):
        '''Creates a dictionary with a key for each column where each entry is the count of the unique values for that column.
        Call it as get_top_values(df, [n]) where df is your dataframe of choice and n (facultative) is the number of rows to return for each column'''
        self.dict_top_values = {}
        for column in df.columns:
            try:
                self.dict_top_values[column]=df[column].value_counts()[:n]
            except:
                self.dict_top_values[column]=df[column].value_counts()[:]
        return self.dict_top_values

    def show_top_values(self, df, column, n=5, group='15min'):
        '''Displays a chart with the top values for the specified column, very similar to count_top_values() but plots the results as well
        Call it as show_top_values(DataFrame, column, [n=5], [group=15min])'''
        # Get the top n values for that column
        try:
            values=df[column].value_counts()[:n]
        except:
            values=df[column].value_counts()[:]
        value_list = list(values.index)

        # Plot them    
        if type(value_list)!=list:
            value_list=[value_list]
        my_list=[]
        for value in value_list:
            my_list.append(df[column][df[column]==value].resample(group).count().rename(value))
        layout = dict(xaxis=dict(
            title=column
            ,rangeslider=dict(visible = True)), legend=dict(x=0, y=1.5))
        pd.concat(my_list, axis=1).iplot(fill='tozeroy', layout=layout)

    def get_dict_by_value(self, df, value):
        '''Searches for a value in the dataframe and shows how many results for each column, returns dict'''
        lis = {}
        for column in df.columns:
            try:
                lis[column] = sum(df[column].str.contains(value))
            except:
                lis[column] = sum(df[column].astype(str).str.contains(value))
        return lis

    def filter_dict_by_value(self, df, value):
        '''Same as get_by_value but plot the results instead of returning them'''
        lis = self.get_dict_by_value(df, value)
        plt.bar(list(lis.keys()), lis.values())
        plt.xticks(rotation=90)

    def filter_by_value(self, df, col, values, exact_match = False):
        '''Returns the dataframe filtered by your value of choice
        Call it as get_by_value(DataFrame, 'column where to search', 'value to search')'''
        my_list = []
        my_list.append(values) if type(values)!=list else my_list == values
        if exact_match == True: #to address the exact match
            my_list = [r'(?<!\w)' + str(v) + r'(?!\w)' for v in my_list]
        try:
            output = df[df[col].str.contains('|'.join(my_list), case=False)]
        except:
            output = df[df[col].astype(str).str.contains('|'.join(my_list), case=False)]
        return output
    
    def get_domain(self, show_query = False):
        '''Simple function which retrieves and cleans the domain lists'''
        df = self.impala_connect(self.queries['query_domains']
                                ,force_reconnect=False
                                , show_query=show_query)[['account_id'
                                                        ,'account_uuid'
                                                        ,'account_name'
                                                        ,'account_active'
                                                        ,'domain_id'
                                                        ,'domain_uuid'
                                                        ,'domain_name'
                                                        ,'domain_active']]
        df.dropna(inplace=True)
        df['domain_id'] = df['domain_id'].astype(int)
        return df

    def search_domain(self, domain_name, show_query = False):
        self.df_domains = self.get_domain(show_query=show_query)
        condition = self.df_domains['domain_name'].str.contains(domain_name.lower(), case=False) | self.df_domains['account_name'].str.contains(domain_name.lower(), case=False)
        filt_df = self.df_domains[condition].sort_values(['account_name', 'domain_name']).dropna()
        display(filt_df)

    def account_id_to_account_name(self, my_id, show_query = False):
        '''Given an account id, give me back the account name'''
        try:
            self.df_domains
        except AttributeError:
            self.df_domains = self.get_domain(show_query=show_query)
        try:
            return self.df_domains[self.df_domains['account_id']==int(my_id)]['account_name'].unique()[0]
        except ValueError:
            return('ID must be a number')
        except IndexError:
            return('Value not found')

    def domain_id_to_domain_name(self, my_id, show_query = False):
        '''Given an domain id, give me back the domain name'''
        try:
            self.df_domains
        except AttributeError:
            self.df_domains = self.get_domain(show_query=show_query)
        try:
            return self.df_domains[self.df_domains['domain_id']==int(my_id)]['domain_name'].unique()[0]
        except ValueError:
            return('ID must be a number')
        except IndexError:
            return('Value not found')
    
    def iplot(self, df):
        '''Simple wrapper for the plotly iplot function, allows to plot any dataframe'''
        df.iplot()

    def show_ip_distribution(self, quantile = 0.999, force_reconnect=False):
        '''Queries for all IP and plots them in ascending order according to how many request each of them has.
        By default top 0.1 percentile is shown, change it with quantile = n, default is 0.999'''
        import ipaddress

        # First query to get the top values
        df = self.impala_connect(self.queries['query_sliced_count'].format(sliced_by ='ip',  where=self.where), force_reconnect=force_reconnect)
        df['ip_int'] = df['ip'].apply(lambda x: int(ipaddress.ip_address(x)))

        # Need to take the top quantile otherwise plotly will choke
        threshold = df.quantile(quantile)[0]
        df_sorted = df[df.iloc[:,1]>=int(threshold)].sort_values(by='ip_int', ascending=False)

        # Plot
        layout = dict(yaxis=dict(tickfont=dict(size=9)))
        df_sorted.iplot(kind='scatter', x="count(ip)", y="ip", mode='markers', size=5, opacity=.9,layout=layout)
