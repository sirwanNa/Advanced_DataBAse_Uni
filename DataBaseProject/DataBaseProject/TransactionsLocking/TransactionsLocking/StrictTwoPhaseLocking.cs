using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TransactionsLocking
{
    public class StrictTwoPhaseLocking : LockingProcess
    {
        public void Run()
        {
            SetRelatedOperations();
            Calculate();
            PrintOutput(methodName:"Strict Two Phase Locking");
        }
        private void SetRelatedOperations()
        {
            var otherOperationsList = new List<Operation>();
            foreach (var group in _operationsList.GroupBy(c => c.Transaction))
            {
                var lockPoint = group.OrderByDescending(p => p.TransactionOrder).FirstOrDefault();
                lockPoint.IsLockPoint = true;
                var scheduleOrder = lockPoint.ScheduleOrder;
                var transactionOrder = lockPoint.TransactionOrder;
                foreach (var item in group)
                {
                    var isExist = otherOperationsList.Exists(c => c.Transaction == item.Transaction && c.DataItem == item.DataItem);
                    if (!isExist)
                    {
                        otherOperationsList.Add(new Operation
                        {
                            DataItem = item.DataItem,
                            Transaction = item.Transaction,
                            ScheduleOrder = ++scheduleOrder,
                            TransactionOrder = ++transactionOrder,
                            OperationType = OperationType.UnLock
                        });
                    }
                }
                otherOperationsList.Add(new Operation
                {
                    Transaction = group.Key,
                    ScheduleOrder = ++scheduleOrder,
                    TransactionOrder = ++transactionOrder,
                    OperationType = OperationType.Commit
                });
            }
            _operationsList.AddRange(otherOperationsList);
        }
        private void Calculate()
        {
            var flag = true;
            var scheduleCounter = 1;
            while (flag)
            {
                var tempOperationsList = new List<Operation>();
                var availableItem = false;
                foreach (var operation in _operationsList)
                {
                    if (operation.OperationType == OperationType.Read || operation.OperationType == OperationType.Write)
                    {
                        if (CheckIsAvailable(operation.DataItem, operation.Transaction, operation.TransactionOrder, operation.OperationType))
                        {
                            availableItem = true;
                            SetLock(operation.DataItem, operation.Transaction, operation.OperationType);
                            InsertToStack(operation, scheduleCounter);
                        }
                        else if (CheckLock(operation.DataItem, operation.Transaction, operation.OperationType).IsLock && CheckAvailability(operation.Transaction, operation.TransactionOrder))
                        {
                            InsertToWaintingList(operation.DataItem, operation.Transaction);
                            tempOperationsList.Add(operation);
                        }
                        else if (!CheckAvailability(operation.Transaction, operation.TransactionOrder))
                        {
                            tempOperationsList.Add(operation);
                        }
                    }
                    else if (operation.OperationType == OperationType.UnLock)
                    {
                        if (CheckAvailability(operation.Transaction, operation.TransactionOrder))
                        {
                            InsertToStack(operation, scheduleCounter);
                            var lockObj = _locksList.FirstOrDefault(p => p.DataItem == operation.DataItem && p.Transaction == operation.Transaction);
                            if(lockObj!=null && lockObj.LockType== LockType.Shared)
                            {
                                ReleaseLock(operation.DataItem, operation.Transaction);
                            }
                            
                        }
                        else
                        {
                            tempOperationsList.Add(operation);
                        }

                    }
                    else if (operation.OperationType == OperationType.Commit)
                    {
                        if(CheckAvailability(operation.Transaction, operation.TransactionOrder))
                        {
                            releaseExlusiveLocks(operation.Transaction);
                            availableItem = true;
                        }

                    }
                    scheduleCounter++;
                }
                if (!availableItem)
                {
                    Result = CalculationResult.Deadlock;
                    break;
                }
                else if (tempOperationsList.Count() == 0)
                {
                    var hasWaitingItem = _waitingsList?.SelectMany(p => p.ItemsList).Count() > 0;
                    if (!hasWaitingItem)
                    {
                        Result = CalculationResult.Done;
                        break;
                    }
                }
                _operationsList = tempOperationsList;

            }
        }
        private void releaseExlusiveLocks(string transaction)
        {           
            _locksList.RemoveAll(c => c.Transaction == transaction && c.LockType == LockType.Exclusive);
        }
    }
}
