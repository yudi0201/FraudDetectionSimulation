using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Microsoft.VisualBasic.FileIO;
using System.Linq;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Runtime.InteropServices;
using System.Xml.Xsl;
using Microsoft.StreamProcessing;

/* Implements fraud detection queries as described in https://wso2.com/whitepapers/fraud-detection-and-prevention-a-data-analytics-approach/
 * Each TransactionRecord represents one single transaction. Each transaction occurs at time "Tick" and
 * has a duration of exactly 1 time unit in the stream of transactions.
 * The other fields of class TransactionRecord - TxnID, ItemNo, Qty, and CardNum represents, respectively,
 * the unique ID of the transaction, the item number of the item purchased, the quantity of purchase, and
 * the card number used for the transaction.
 */
namespace FraudDetectionTrill
{
    class Program
    {
        class TransactionRecord
        {
            public long Tick;
            public long TxnID;
            public long ItemNo;
            public long Qty;
            public long CardNum;

            public TransactionRecord(long tick, long txnID, long itemNo, long qty, long cardNum)
            {
                this.Tick = tick;
                this.TxnID = txnID;
                this.ItemNo = itemNo;
                this.Qty = qty;
                this.CardNum = cardNum;
            }
        }

        class MyObservable : IObservable<TransactionRecord>
        {
            public IDisposable Subscribe(IObserver<TransactionRecord> observer)
            {
                using (var reader = new StreamReader(@"/root/FraudDetectionSimulation/synthetic_txn_data_100_thousand_UNIX.csv"))
                //using (var reader = new StreamReader(@"C:\Users\yudis\Documents\university\Summer2021\Code\FraudDetectionSimulation\synthetic_txn_data_1_thousand_UNIX.csv"))
                
                {
                  reader.ReadLine();
                  while (!reader.EndOfStream)
                  {
                      var line = reader.ReadLine();
                      var values = line.Split(',');
                      var data = new TransactionRecord((long)Convert.ToDouble(values[0]), long.Parse(values[1]), long.Parse(values[2]), long.Parse(values[3]), long.Parse(values[4]));
                      observer.OnNext(data);

                  }
                }
                observer.OnCompleted();
                return Disposable.Empty;
            }
        }
        static void Main(string[] args)
        {
            
            //var transactionRecordObservable = new[]
            //{
                //This should be done in a separate file, but I still need to learn how to read files in C#.
            //    new TransactionRecord(0, 1, 12345, 5, 6789),
            //    new TransactionRecord(2, 2, 23456, 10, 4046),
            //    new TransactionRecord(3, 3, 34567, 7, 5874),
            //    new TransactionRecord(3, 4, 12345, 10, 3745),
            //    new TransactionRecord(6, 5, 23456, 6, 8083),
            //    new TransactionRecord(7, 6, 34567, 20, 6789),
            //    new TransactionRecord(7, 7, 12345, 15, 9785), //high velocity
            //    new TransactionRecord(9, 8, 34567, 13, 1545),
            //    new TransactionRecord(10, 9, 23456, 9, 9785), //high velocity
            //    new TransactionRecord(10, 10, 12345, 10, 9785), //high velocity
            //    new TransactionRecord(12, 11, 23456, 5, 1545),
            //    new TransactionRecord(14, 12, 34567, 8, 1545),
            //    new TransactionRecord(14, 13, 23456, 100, 4046), //large qty
            //    new TransactionRecord(15, 14, 12345, 100, 4046), //large qty
            //    new TransactionRecord(17, 15, 23456, 14, 8083),
            //    new TransactionRecord(19, 16, 34567, 18, 2953),
            //    new TransactionRecord(20, 17, 34567, 100, 3875) //large qty
            //}.ToObservable();
            
            //var transactionRecordStreamable =
            //    transactionRecordObservable.Select(e => StreamEvent.CreateInterval(e.Tick, e.Tick + 1, e))
            //        .ToStreamable(DisorderPolicy.Drop());
            
            var sw = new Stopwatch();
            sw.Start();

            var transactionRecordObservable = new MyObservable();
            var transactionRecordStreamable =
                transactionRecordObservable.ToTemporalStreamable(e => e.Tick, e => e.Tick + 1);
            
            //var allTransactions =
            //    transactionRecordStreamable.Select(e => new{e.TxnID, e.ItemNo, e.Qty, e.CardNum}); //used to display all transactions initiated above.
            
            //Large Transaction Quantities
            //Computes a moving average and stdev (with window size given by largeQtyWindowSize) of the quantity purchased for each individual item
            //for all the transactions of that item which fall within the window. The moving average is computed every second (i.e.continuously).
            //If a new transaction of that item has a quantity that exceeds the value of (moving average + 3 * stdev) in the previous window,
            //that new transaction is reported as a potentially fraudulent transaction.


            
            //int largeQtyWindowSize = 50;
            //var qtyStat = transactionRecordStreamable.GroupApply(e => e.ItemNo,
            //    s => s.HoppingWindowLifetime(largeQtyWindowSize, 1)
            //        .Aggregate(w => w.Average(e => e.Qty),
            //            w => w.StandardDeviation(e => e.Qty),
            //            (avgQty, stdevQty) => new {avgQty, stdevQty}),
            //    (g, p) => new {itemNo = g.Key, qtyStat = p}) //computes the average and stdev  
            //    .ShiftEventLifetime(1); //shift one unit to ensure that when joined with the original stream, each individual transaction in the original stream is mapped 
                                        //with the average and stdev computed previously, excluding the present transaction which is being
                                        //examined for potential fraud.   
                                        //Question: is there a way to remove a fraudulent event once it is detected so that it doesn't contribute
                                        //to future averages and stdev's?

            //var combinedWithOrig = qtyStat.Join(transactionRecordStreamable, e => e.itemNo, e => e.ItemNo,
            //    (left, right) => new {left.itemNo, left.qtyStat, right.TxnID, right.Qty});

            //var largeQty = combinedWithOrig.Where(e =>
            //    e.Qty > e.qtyStat.avgQty + 3 * e.qtyStat.stdevQty)
            //    .Select(e=>new{e.TxnID, e.itemNo, e.Qty, e.qtyStat}); //gives final results of potentially
                                                                                    //fraudulent transactions.
            
            // High Velocity Transactions
            // Detects high frequency purchase from the same card number. 
            // If the same card number completes "maxTxns" or more transactions within "windowSize", this query will return
            // all of the transactions made by that card within that window. 
            
            int windowSize = 10;
            int maxTxns = 3;
            var txnByCard = transactionRecordStreamable.GroupApply(e => e.CardNum,
                s => s.HoppingWindowLifetime(windowSize, 1).Count(),
                (g, p) => new {cardNum = g.Key, numTxn = p}); //group transactions by card number and counts the number of transactions per window.
            
            var txnExtended = transactionRecordStreamable.ExtendLifetime(windowSize); 

            var highVelocityTxn = txnByCard.Join(txnExtended, e => e.cardNum, e => e.CardNum,
                (left, right) => new {left.cardNum, left.numTxn, right})
                .Where(e=>(int) e.numTxn >= maxTxns)
                .Select(e=>new{e.cardNum,e.right.TxnID}); //final results
            
            //Question: Is there a better way to get all the offending high-frequency transactions with their origianl start and end times?

            //highVelocityTxn
            //    .ToStreamEventObservable()
            //    .ForEachAsync(e => Console.WriteLine(e.ToString()));

            highVelocityTxn
                .ToStreamEventObservable()
                .Wait();
            
            sw.Stop();
            Console.WriteLine(sw.Elapsed.TotalSeconds);
        }
    }
    


}