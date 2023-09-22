using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TransactionsLocking
{
    public class RigorousTwoPhaseLocking : LockingProcess
    {
        public void Run()
        {
            SetRelatedOperations();
            Calculate();
            PrintOutput("Rigorous Two Phase Locking");
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
                            availableItem = true;
                        }
                        else
                        {
                            tempOperationsList.Add(operation);
                        }

                    }
                    else if (operation.OperationType == OperationType.Commit)
                    {
                        if (CheckAvailability(operation.Transaction, operation.TransactionOrder))
                        {
                            releaseLocks(operation.Transaction);
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
        private void releaseLocks(string transaction)
        {
            _locksList.RemoveAll(c => c.Transaction == transaction);
        }
    }
}
