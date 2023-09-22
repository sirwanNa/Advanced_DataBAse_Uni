using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
namespace TransactionsLocking
{
    public class TimeStampLocking: LockingProcess
    {
        private List<TimeStapModelDataItem> _w_TimeStampsList;
        private List<TimeStapModelDataItem> _r_TimeStampsList;
        private List<TimeStampModelTransaction> _timeStampsList;
        public TimeStampLocking()
        {
            _w_TimeStampsList = new List<TimeStapModelDataItem>();
            _r_TimeStampsList = new List<TimeStapModelDataItem>();
            _timeStampsList = new List<TimeStampModelTransaction>();
        }
        public void RollBack(string transaction)
        {
            ReleaseLocks(transaction);
            var stack = _stacksList.FirstOrDefault(p => p.Transaction == transaction);
            if (stack != null && stack.StackItemsList!=null)
            {
                stack.StackItemsList = new List<Operation>();
            }
            UpdateScheduleCounter(transaction);

        }
        private void UpdateScheduleCounter(string transaction)
        {
            var lastTimeStamp = _timeStampsList.OrderByDescending(p => p.TimeStamp).FirstOrDefault()?.TimeStamp;
            var timeStamp= _timeStampsList.FirstOrDefault(p => p.Transaction == transaction);
            if (timeStamp != null)
            {
                timeStamp.TimeStamp = lastTimeStamp != null ? lastTimeStamp.Value + 1 : 1;
            }
        }

        private void ReleaseLocks(string transaction)
        {
            if (_locksList != null)
            {
                var locksList = _locksList.Where(p => p.Transaction == transaction);
                foreach (var lockObj in locksList)
                {
                    _locksList.Remove(lockObj);
                }
            }
        } 
        public void Run()
        {
            SetTimeStamps();
            SetLockPoints();
            Calculate();
            PrintOutput(methodName:"Based Time Stamp Protocol");
        }
        private void SetLockPoints()
        {        
            foreach (var group in _operationsList.GroupBy(c => c.Transaction))
            {
                var lockPoint = group.OrderByDescending(p => p.TransactionOrder).FirstOrDefault();
                lockPoint.IsLockPoint = true;
            }           
        }
        private void SetTimeStamps()
        {
            var transactionsList = _operationsList?.GroupBy(p => p.Transaction).Select(p => p.Key).OrderBy(p=>p);
            var counter = 1;
            foreach(var transaction in transactionsList)
            {
                _timeStampsList.Add(new TimeStampModelTransaction
                {
                    Transaction=transaction,
                    TimeStamp=counter++
                });
            }
        }
        public void Calculate()
        {
            //var tempOperationsList = _operationsList;
            var scheduleCounter = 1;
            while (true)
            {
                foreach(var operation in _operationsList)
                {
                    var transactionTimeStamp = GetTimeStamp(operation.Transaction);
                    if (operation.OperationType== OperationType.Read)
                    {
                        var w_TimeStamp=_w_TimeStampsList.FirstOrDefault(p => p.DataItem == operation.DataItem);
                        if(w_TimeStamp!=null && transactionTimeStamp < w_TimeStamp.TransactionTimeStamp)
                        {
                            RollBack(operation.Transaction);
                        }
                        else
                        {
                            UpdateReadTimeStamp(operation.DataItem, transactionTimeStamp); 
                            InsertToStack(operation, scheduleCounter);
                        }
                    }
                    else if(operation.OperationType== OperationType.Write)
                    {
                        var r_TimeStamp = _r_TimeStampsList.FirstOrDefault(p => p.DataItem == operation.DataItem);
                        var w_TimeStamp = _w_TimeStampsList.FirstOrDefault(p => p.DataItem == operation.DataItem);
                        if (r_TimeStamp!=null && transactionTimeStamp < r_TimeStamp.TransactionTimeStamp)
                        {
                            RollBack(operation.Transaction);
                        }
                        else if (w_TimeStamp != null && transactionTimeStamp < w_TimeStamp.TransactionTimeStamp)
                        {
                            RollBack(operation.Transaction);
                        }
                        else
                        {
                            UpdateWriteTimeStamp(operation.DataItem, transactionTimeStamp);
                            InsertToStack(operation, scheduleCounter);
                       }
                    }
                    scheduleCounter++;
                }
                foreach(var stack in _stacksList)
                {
                    if (stack.StackItemsList != null && stack.StackItemsList.Count()>0 && stack.StackItemsList[stack.StackItemsList.Count()-1].IsLockPoint)
                    {
                        _operationsList.RemoveAll(p => p.Transaction == stack.Transaction);                       
                    }
                }
                if (_operationsList.Count() == 0)
                {
                    Result = CalculationResult.Done;
                    break;
                }
                
            }
        }
        private int GetTimeStamp(string transaction)
        {
            var timeStamp = _timeStampsList.FirstOrDefault(p => p.Transaction == transaction);
            return timeStamp != null ? timeStamp.TimeStamp : 0;
        }
        private void UpdateReadTimeStamp(string dataItem,int scheduleOrder)
        {
            var r_TimeStamp = _r_TimeStampsList.FirstOrDefault(p => p.DataItem == dataItem);
            if (r_TimeStamp == null)
            {
                r_TimeStamp = new TimeStapModelDataItem
                {
                    DataItem= dataItem,
                    TransactionTimeStamp= scheduleOrder
                };
                _r_TimeStampsList.Add(r_TimeStamp);
            }
            else if (scheduleOrder > r_TimeStamp.TransactionTimeStamp)
            {
                r_TimeStamp.TransactionTimeStamp = scheduleOrder;
            }
            
        }
        private void UpdateWriteTimeStamp(string dataItem, int scheduleOrder)
        {
            var w_TimeStamp = _w_TimeStampsList.FirstOrDefault(p => p.DataItem == dataItem);
            if (w_TimeStamp == null)
            {
                w_TimeStamp = new TimeStapModelDataItem
                {
                    DataItem = dataItem,
                    TransactionTimeStamp = scheduleOrder
                };
                _w_TimeStampsList.Add(w_TimeStamp);
            }
            else if(scheduleOrder> w_TimeStamp.TransactionTimeStamp)
            {
                w_TimeStamp.TransactionTimeStamp = scheduleOrder;
            }
        }
        //private void insertToStack(Operation operation)
        //{
        //    var stack = _stacksList.FirstOrDefault(c => c.Transaction == operation.Transaction);
        //    if (stack == null)
        //    {
        //        stack = new TransactionStackModel
        //        {
        //            Transaction = operation.Transaction,
        //            StackItemsList = new List<Operation>()
        //        };
        //        _stacksList.Add(stack);
        //    }
        //    stack.StackItemsList.Add(new Operation
        //    {
        //        DataItem = operation.DataItem,
        //        OperationType = operation.OperationType,
        //        ScheduleOrder = operation.ScheduleOrder,
        //        Transaction = operation.Transaction,
        //        TransactionOrder = operation.TransactionOrder,
        //        IsLockPoint = operation.IsLockPoint
        //    });
        //}

    }
    public class TimeStapModelDataItem
    {
        public string DataItem { get; set; }
        public int TransactionTimeStamp { get; set; } 
    }
    public class TimeStampModelTransaction
    {
        public string Transaction { get; set; }
        public int TimeStamp { get; set; }
    }
}
