using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.OracleClient;
using System.IO;
using System.Net.Mail;
using System.Text;
using System.Threading;
using System.Timers;

namespace HC_Poller2_2
{
    /// <summary>
    /// This program will read from standard Intergraph tables,
    /// Write the information into temp tables
    /// Then combine those temp tables into a master table
    /// While clearing the temp tables and
    /// Removing unneeded calls from the master table
    /// </summary>
    // Program: Hot Calls Poller Version 2 Write 1
    // Author: Tony Dunsworth
    // Date Created: 2013-01-03
    // Switched from odp.net to system.data.oracleclient as it seems to behave better on the equipment even if it is being deprecated at some future time.
    class Program
    {
        // Creates the database connection string. The details are placed elsewhere for security.
        public static string hcStr = ConfigurationManager.ConnectionStrings["hotcall"].ConnectionString;

        // Main function
        static void Main(string[] args)
        {
            System.Timers.Timer poller = new System.Timers.Timer(120000); // set the timer to 120 seconds
            poller.Elapsed += new ElapsedEventHandler(poller_Elapsed); // what to do when the timer is up
            poller.Enabled = true;
            Console.WriteLine("Press the enter key to exit"); // allows for a graceful exit for maintenance
            Console.ReadLine(); // this waits for the return key and then exits when it hears it.
            GC.KeepAlive(poller); // this prevents the garbage collector from taking the memory space back and forcing the program closed.
        }

        // The poller_Elapsed function is invoked by the ElapsedEventHandler to
        // query the database and write the collected data into the 
        // jc_hc_curent table for the Logic to evaluate it.
        private static void poller_Elapsed(object sender, ElapsedEventArgs e)
        {
            // starting and ending with WriteLine calls to give an administrator a visual cue it's working and to see how long it's taking for the routine to run.
            Console.WriteLine("Poller entering at " + DateTime.Now.ToString("HH:mm:ss")); 
            // These functions poll Intergraph tables for the data. We're using the old aeven and event views for now 
            // while we rework the queries to address the new tables and those changes for 9.1.1
            CollectCurentCalls();
            CollectCurentComments();
            CollectCurentCount();
            // These functions write the data from our temp tables into the jc_hc_curent table
            WriteCurentCalls();
            WriteCurentComments();
            WriteCurentCount();
            // These functions clear the data from our temp tables so the next poll will be clean
            PurgeCurentCalls();
            PurgeCurentComments();
            PurgeCurentCount();
            // These functions check on the jc_hc_curent table and delete calls over 2 hours old or calls which have been closed.
            CleanJHCCalls();
            DeleteJHCCalls();
            Console.WriteLine("Poller exiting at " + DateTime.Now.ToString("HH:mm:ss"));
        }

        // This function grabs the basic call information for all calls less than 8 minutes old.
        private static void CollectCurentCalls()
        {
            // The commented WriteLine functions are used for debugging to see when the program steps in and out of this function.
            // These functions are duplicated in each of the method calls to provide a longer debugging trail in the console.
            //Console.WriteLine("Step 1 entering at " + DateTime.Now.ToString("HH:mm:ss"));
            // First insert statement, grabs from aeven and event views to get the information.
            // Tested in Oracle so the query is stable.
            string s1 = "INSERT INTO hc_curent_temp(eid, ag_id, tycod, sub_tycod, ad_ts, udts, xdts, num_1, estnum, edirpre, efeanme, efeatyp, xstreet1, xstreet2, esz) SELECT DISTINCT a.eid, a.ag_id, a.tycod, a.sub_tycod, a.ad_ts, a.udts, a.xdts, a.num_1, e.estnum, e.edirpre, e.efeanme, e.efeatyp, e.xstreet1, e.xstreet2, a.esz FROM aeven a, event e WHERE a.ad_ts > TO_CHAR(systimestamp-8/1440, 'YYYYMMDDHH24MISS') AND a.open_and_curent='T' AND e.curent='T' AND a.eid=e.eid";
            OracleConnection cxn1 = new OracleConnection(hcStr);
            OracleCommand cmd1 = new OracleCommand(s1, cxn1);
            cxn1.Open();
            // Try to run the query
            try { cmd1.ExecuteNonQuery(); }
            // moved the exception handling out of the main code for compactness and better reuse.
            // If there are any Oracle based errors, the first exception handler will process them, if anything else, the second will catch it
            catch (OracleException ox) { OracleErrorReporter(ox); }
            catch (Exception ex) { ErrorReporter(ex); }
            finally
            {
                // Close the connection out
                cxn1.Close();
                //Console.WriteLine("Step 1 exiting at {0}", DateTime.Now.ToString("HH:mm:ss"));
            }
        }

        // This function grabs the comments from the calls which are less than 8 minutes old.
        private static void CollectCurentComments()
        {
            //Console.WriteLine("Step 2 entering at " + DateTime.Now.ToString("HH:mm:ss"));
            // The query not only collects the comments, but sorts them by their position in the creation process so the final 
            // comment string reads more coherently. The substr function takes the comments and concatenates them together as a longer string
            // to do this, the varchar2_ntt type and the to_ntt casting function are created on the database side so this can be done in the query
            // rather than being serialized in the code itself. 
            // The NOT(REGEXP_LIKE) restrictions have been customed to remove certain internal only comments from being read/seen by anyone outside
            // of the dispatch floor.
            string s2 = @"insert into hc_comment_temp(eid, num_1, ad_ts, comments) SELECT a.eid, a.num_1, a.ad_ts, substr(TO_NTT(CAST(COLLECT(e.comm ORDER BY e.cdts, e.lin_grp, e.lin_ord) AS varchar2_ntt)),1,4000) FROM aeven a, evcom e WHERE a.eid=e.eid AND a.ad_ts > TO_CHAR(systimestamp-8/1440, 'YYYYMMDDHH24MISS') AND a.open_and_curent='T' AND e.comm_key = 0 AND NOT(REGEXP_LIKE(e.comm, '^[A-Z,/]{3,}(\s[A-Z]{2,})?(\s[A-Z]{2,})?:') OR REGEXP_LIKE(e.comm, '[A-Z,^0-9]{3,}/') OR REGEXP_LIKE(e.comm, 'END OF (K)?DOR RESPONSE') OR REGEXP_LIKE(e.comm, '-{3,}') OR REGEXP_LIKE(e.comm, '10-[0-9]{2}') OR REGEXP_LIKE(e.comm, 'LICENSE:') OR REGEXP_LIKE(e.comm, '\*{3,}') OR REGEXP_LIKE(e.comm, 'Field Event')) GROUP BY a.eid, a.num_1, a.ad_ts";
            OracleConnection cxn2 = new OracleConnection(hcStr);
            OracleCommand cmd2 = new OracleCommand(s2, cxn2);
            cxn2.Open();
            try { cmd2.ExecuteNonQuery(); }
            catch (OracleException ox) { OracleErrorReporter(ox); }
            catch (Exception ex) { ErrorReporter(ex); }
            finally
            {
                cxn2.Close();
                //Console.WriteLine("Step 2 exiting at {0}", DateTime.Now.ToString("HH:mm:ss"));
            }
        }

        // This function calculates and sets up the count of units arrived at a call less than 20 minutes old.
        private static void CollectCurentCount()
        {
            //Console.WriteLine("Step 3 entering at " + DateTime.Now.ToString("HH:mm:ss"));
            string s3 = @"insert into hc_unitcount_temp(eid, num_1, ag_id, ad_ts, unit_count) select a.eid, a.num_1, a.ag_id, a.ad_ts, count(distinct u.unid) from aeven a, un_hi u where a.eid=u.eid and a.num_1=u.num_1 and a.ag_id=u.ag_id and a.open_and_curent='T' and a.ad_ts > to_char(systimestamp-20/1440, 'YYYYMMDDHH24MISS') and u.unit_status='AR' group by a.eid, a.num_1, a.ag_id, a.ad_ts";
            OracleConnection cxn3 = new OracleConnection(hcStr);
            OracleCommand cmd3 = new OracleCommand(s3, cxn3);
            cxn3.Open();
            try { cmd3.ExecuteNonQuery(); }
            catch (OracleException ox) { OracleErrorReporter(ox); }
            catch (Exception ex) { ErrorReporter(ex); }
            finally
            {
                cxn3.Close();
                //Console.WriteLine("Step 3 exiting at {0}", DateTime.Now.ToString("HH:mm:dd);
            }
        }

        // This function will merge the current contents of the hc_curent_temp table into the jc_hc_curent table, if there is a matching entry
        // (matching on eid, ad_ts, and num_1).
        // If there is no match then the call details are inserted into the table to form a new record.
        private static void WriteCurentCalls()
        {
            //Console.WriteLine("Step 4 entering at " + DateTime.Now.ToString("HH:mm:ss"));
            string s4 = "merge into jc_hc_curent j using (select distinct eid, ag_id, tycod, sub_tycod, ad_ts, udts, xdts, num_1, estnum, edirpre, efeanme, efeatyp, xstreet1, xstreet2, esz FROM hc_curent_temp) h on (j.eid = h.eid and j.num_1 = h.num_1 and j.ad_ts = h.ad_ts) when matched then update set j.udts = h.udts, j.xdts = h.xdts, j.ag_id = h.ag_id, j.tycod = h.tycod, j.sub_tycod = h.sub_tycod, j.estnum = h.estnum, j.edirpre = h.edirpre, j.efeanme = h.efeanme, j.efeatyp = h.efeatyp, j.xstreet1 = h.xstreet1, j.xstreet2 = h.xstreet2, j.esz = h.esz when not matched then insert (eid, ag_id, tycod, sub_tycod, ad_ts, udts, xdts, num_1, estnum, edirpre, efeanme, efeatyp, xstreet1, xstreet2, esz) VALUES (h.eid, h.ag_id, h.tycod, h.sub_tycod, h.ad_ts, h.udts, h.xdts, h.num_1, h.estnum, h.edirpre, h.efeanme, h.efeatyp, h.xstreet1, h.xstreet2, h.esz)";
            OracleConnection cxn4 = new OracleConnection(hcStr);
            OracleCommand cmd4 = new OracleCommand(s4, cxn4);
            cxn4.Open();
            try { cmd4.ExecuteNonQuery(); }
            catch (OracleException ox) { OracleErrorReporter(ox); }
            catch (Exception ex) { ErrorReporter(ex); }
            finally
            {
                cxn4.Close();
                //Console.WriteLine("Step 4 exiting at {0}", DateTime.Now.ToString("HH:mm:ss"));
            }
        }

        // This function merges the current contents of the hc_comment_temp table into the jc_hc_curent table, if there is a matching entry.
        // If there is no match then nothing happens to the data and it will be purged when the purge function is run.
        private static void WriteCurentComments()
        {
            //Console.WriteLine("Step 5 entering at " + DateTime.Now.ToString("HH:mm:ss"));
            string s5 = "merge into jc_hc_curent j using (select distinct eid, num_1, ad_ts, comments from hc_comment_temp) c on (j.eid = c.eid and j.ad_ts = c.ad_ts and j.num_1 = c.num_1) when matched then update set j.comments = c.comments";
            OracleConnection cxn5 = new OracleConnection(hcStr);
            OracleCommand cmd5 = new OracleCommand(s5, cxn5);
            cxn5.Open();
            try { cmd5.ExecuteNonQuery(); }
            catch (OracleException ox) { OracleErrorReporter(ox); }
            catch (Exception ex) { ErrorReporter(ex); }
            finally
            {
                cxn5.Close();
                //Console.WriteLine("Step 5 exiting at {0}", DateTime.Now.ToString("HH:mm:ss"));
            }
        }

        // This function merges the current contents of the hc_unitcount_temp table into the jc_hc_curent table; if there is a matching entry.
        // If there is no match, then nothing happens to the data and it will be purged when that function is run.
        private static void WriteCurentCount()
        {
            //Console.WriteLine("Step 6 entering at " + DateTime.Now.ToString("HH:mm:ss"));
            string s6 = "merge into jc_hc_curent j using (select distinct eid, ad_ts, num_1, ag_id, unit_count from hc_unitcount_temp) u on (j.eid=u.eid and j.ad_ts=u.ad_ts and j.num_1=u.num_1 and j.ag_id=u.ag_id ) when matched then update set j.unit_count=u.unit_count";
            OracleConnection cxn6 = new OracleConnection(hcStr);
            OracleCommand cmd6 = new OracleCommand(s6, cxn6);
            cxn6.Open();
            try { cmd6.ExecuteNonQuery(); }
            catch (OracleException ox) { OracleErrorReporter(ox); }
            catch (Exception ex) { ErrorReporter(ex); }
            finally
            {
                cxn6.Close();
                //Console.WriteLine("Step 6 exiting at {0}", DateTime.Now.ToString("HH:mm:ss"));
            }
        }

        // This function purges out the contents of the hc_curent_temp table. This keeps memory sizes lower and allows for quicker processing of the 
        // gathering and comparison.
        private static void PurgeCurentCalls()
        {
            //Console.WriteLine("Step 7 entering at " + DateTime.Now.ToString("HH:mm:ss"));
            string s7 = "truncate table hc_curent_temp reuse storage";
            OracleConnection cxn7 = new OracleConnection(hcStr);
            OracleCommand cmd7 = new OracleCommand(s7, cxn7);
            cxn7.Open();
            try { cmd7.ExecuteNonQuery(); }
            catch (OracleException ox) { OracleErrorReporter(ox); }
            catch (Exception ex) { ErrorReporter(ex); }
            finally
            {
                cxn7.Close();
                //Console.WriteLine("Step 7 exiting at {0}", DateTime.Now.ToString("HH:mm:ss"));
            }
        }

        // This function purges out the contents of the hc_comment_temp table. This keeps memory demands lower and allows for quicker
        // gathering and comparison of data.
        private static void PurgeCurentComments()
        {
            //Console.WriteLine("Step 8 entering at " + DateTime.Now.ToString("HH:mm:ss"));
            string s8 = "truncate table hc_comment_temp reuse storage";
            OracleConnection cxn8 = new OracleConnection(hcStr);
            OracleCommand cmd8 = new OracleCommand(s8, cxn8);
            cxn8.Open();
            try { cmd8.ExecuteNonQuery(); }
            catch (OracleException ox) { OracleErrorReporter(ox); }
            catch (Exception ex) { ErrorReporter(ex); }
            finally
            {
                cxn8.Close();
                //Console.WriteLine("Step 8 exiting at {0}", DateTime.Now.ToString("HH:mm:ss"));
            }
        }

        // This function purgest out the contents of the hc_unitcount_temp table. This keeps memory demands lower and allows for quicker
        // gathering and comparison of data.
        private static void PurgeCurentCount()
        {
            //Console.WriteLine("Step 9 entering at " + DateTime.Now.ToString("HH:mm:ss"));
            string s9 = "truncate table hc_unitcount_temp reuse storage";
            OracleConnection cxn9 = new OracleConnection(hcStr);
            OracleCommand cmd9 = new OracleCommand(s9, cxn9);
            cxn9.Open();
            try { cmd9.ExecuteNonQuery(); }
            catch (OracleException ox) { OracleErrorReporter(ox); }
            catch (Exception ex) { ErrorReporter(ex); }
            finally
            {
                cxn9.Close();
                //Console.WriteLine("Step 9 exiting at {0}", DateTime.Now.ToString("HH:mm:ss"));
            }
        }

        // This function deletes completed calls (calls where the closed datetime stamp (xdts) is not empty) so they will not be re-examined.
        private static void CleanJHCCalls()
        {
            //Console.WriteLine("Step 10 entering at " + DateTime.Now.ToString("HH:mm:ss"));
            string s10 = "delete from jc_hc_curent where xdts is not null";
            OracleConnection cxn10 = new OracleConnection(hcStr);
            OracleCommand cmd10 = new OracleCommand(s10, cxn10);
            cxn10.Open();
            try { cmd10.ExecuteNonQuery(); }
            catch (OracleException ox) { OracleErrorReporter(ox); }
            catch (Exception ex) { ErrorReporter(ex); }
            finally
            {
                cxn10.Close();
                //Console.WriteLine("Step 10 exiting at {0}", DateTime.Now.ToString("HH:mm:ss"));
            }
        }

        // This function deletes all open call over two hours old from the jc_hc_curent table so they will not be re-examined. 
        private static void DeleteJHCCalls()
        {
            //Console.WriteLine("Step 11 entering at " + DateTime.Now.ToString("HH:mm:ss"));
            string s11 = "delete from jc_hc_curent where substr(ad_ts,1,14) < to_char(systimestamp-2/24, 'YYYYMMDDHH24MISS')";
            OracleConnection cxn11 = new OracleConnection(hcStr);
            OracleCommand cmd11 = new OracleCommand(s11, cxn11);
            cxn11.Open();
            try { cmd11.ExecuteNonQuery(); }
            catch (OracleException ox) { OracleErrorReporter(ox); }
            catch (Exception ex) { ErrorReporter(ex); }
            finally
            {
                cxn11.Close();
                //Console.WriteLine("Step 11 exiting at {0}", DateTime.Now.ToString("HH:mm:ss"));
            }
        }

        // This function handles all Oracle derived exceptions and sends out an email to Carter and Tony to notify them of the same
        private static void OracleErrorReporter(OracleException ox)
        {
            MailMessage orMsg = new MailMessage();
            orMsg.From = new MailAddress("jcso.exception@jocogov.org", "Oracle Exception");
            orMsg.To.Add(new MailAddress("tony.dunsworth@jocogov.org", "Tony Dunsworth"));
            orMsg.To.Add(new MailAddress("carter.wetherington@jocogov.org", "Carter Wetherington"));
            orMsg.Subject = "Oracle Poller Exception";
            orMsg.Body = "Oracle threw the following exception: " + ox.ToString() + " at " + DateTime.Now.ToString("yyyy-MM-dd HH:mm");
            SmtpClient orPost = new SmtpClient();
            orPost.Send(orMsg);

            FileStream pollErrorLog = null;
            pollErrorLog = File.Open(@"C:\Temp\pollerErrorLog.txt", FileMode.Append, FileAccess.Write);
            StreamWriter pollErrorWrite = new StreamWriter(pollErrorLog);
            pollErrorWrite.WriteLine("Oracle threw the following exception {0} at {1}", ox.ToString(), DateTime.Now.ToString("yyyy-MM-dd HH:mm"));
            pollErrorWrite.Close();
        }

        // This function handles all other exceptions by sending out an email to Carter and Tony.
        private static void ErrorReporter(Exception ex)
        {
            MailMessage exMsg = new MailMessage();
            exMsg.From = new MailAddress("jcso.exception@jocogov.org", "Hot Call Exception");
            exMsg.To.Add(new MailAddress("tony.dunsworth@jocogov.org", "Tony Dunsworth"));
            exMsg.To.Add(new MailAddress("carter.wetherington@jocogov.org", "Carter Wetherington"));
            exMsg.Subject = "Hot Calls Poller Exception";
            exMsg.Body = "The program threw the following exception: " + ex.ToString() + " at " + DateTime.Now.ToString("yyyy-MM-dd HH:mm");
            SmtpClient exPost = new SmtpClient();
            exPost.Send(exMsg);


            FileStream pollErrorLog = null;
            pollErrorLog = File.Open(@"C:\Temp\pollerErrorLog.txt", FileMode.Append, FileAccess.Write);
            StreamWriter pollErrorWrite = new StreamWriter(pollErrorLog);
            pollErrorWrite.WriteLine("Oracle threw the following exception {0} at {1}", ex.ToString(), DateTime.Now.ToString("yyyy-MM-dd HH:mm"));
            pollErrorWrite.Close();
        }
    }
}
