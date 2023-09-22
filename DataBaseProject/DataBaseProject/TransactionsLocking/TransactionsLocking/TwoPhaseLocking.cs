using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TransactionsLocking
{
    public class TwoPhaseLocking: LockingProcess
    {
        public void Run()
        {
            SetLockPoints();
            Calculate();
            PrintOutput("Two Phase Locking");
        }
        private void SetLockPoints()
        {
            var unLockList = new List<Operation>();
            foreach (var group in _operationsList.GroupBy(c => c.Transaction))
            {
                var lockPoint = group.OrderByDescending(p => p.TransactionOrder).FirstOrDefault();
                lockPoint.IsLockPoint = true;
                var scheduleOrder = lockPoint.ScheduleOrder;
                var transactionOrder = lockPoint.TransactionOrder;
                foreach (var item in group)
                {
                    var isExist = unLockList.Exists(c => c.Transaction == item.Transaction && c.DataItem == item.DataItem);
                    if (!isExist)
                    {
                        unLockList.Add(new Operation
                        {
                            DataItem = item.DataItem,
                            Transaction = item.Transaction,
                            ScheduleOrder = ++scheduleOrder,
                            TransactionOrder = ++transactionOrder,
                            OperationType = OperationType.UnLock
                        });
                    }
                }
            }
            _operationsList.AddRange(unLockList);
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
                            ReleaseLock(operation.DataItem, operation.Transaction);
                            availableItem = true;
                        }
                        else
                        {
                            tempOperationsList.Add(operation);
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
                    //else if(hasWaitingItem && !availableItem)
                    //{
                    //    Result = CalculationResult.Deadlock;
                    //    break;
                    //}
                }
                _operationsList = tempOperationsList;

            }
        }
    }
}
