using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TransactionsLocking
{
    public abstract class LockingProcess
    {
        protected List<LockModel> _locksList;
        protected List<Operation> _operationsList;
        protected List<TransactionStackModel> _stacksList;
        protected List<WaitingModel> _waitingsList { get; set; }
        public CalculationResult Result { get; set; }
        public List<WaitingModel> WaitingsList { get { return _waitingsList; } }
        public LockingProcess()
        {
            _locksList = new List<LockModel>();
            _stacksList = new List<TransactionStackModel>();
            _waitingsList = new List<WaitingModel>();
            // _operationsList = operationsList.OrderBy(p => p.ScheduleOrder).ToList();
        }
        public void Initiate(string filePath)
        {
            _operationsList = new List<Operation>();
            if (System.IO.File.Exists(filePath))
            {
                var lines = System.IO.File.ReadAllLines(filePath);
                foreach (var line in lines)
                {
                    var arrString = line.Split(',');
                    if (arrString != null && arrString.Count() >= 4)
                    {
                        _operationsList.Add(new Operation
                        {
                            DataItem = arrString[2],
                            OperationType = arrString[1] == "R" ? OperationType.Read : OperationType.Write,
                            Transaction = arrString[0],
                            ScheduleOrder = int.Parse(arrString[3])
                        });
                    }
                }
            }
            SetTransactionOrder();
        }
     

        private void SetTransactionOrder()
        {
            foreach (var group in _operationsList.GroupBy(c => c.Transaction))
            {
                var counter = 1;
                foreach (var item in group)
                {
                    item.TransactionOrder = counter++;
                }
            }
        }


        protected void PrintOutput(string methodName)
        {

            var operationsList = _stacksList.SelectMany(p => p.StackItemsList).OrderBy(c => c.ScheduleOrder);
            var transactionsList = operationsList.GroupBy(c => c.Transaction).Select(c => c.Key).OrderBy(c => c).ToArray();

            Console.WriteLine($"\n   /************ {methodName} ************/\n");         
            foreach (var opertion in operationsList)
            {
                var transactionIndex = Array.IndexOf(transactionsList, opertion.Transaction);
                var tab = CreateTab(transactionIndex);
                var instruction = string.Empty;
                if (opertion.OperationType == OperationType.Read)
                    instruction = $"Read({opertion.DataItem})";
                else if (opertion.OperationType == OperationType.Write)
                    instruction = $"Write({opertion.DataItem})";
                else
                    instruction = $"Undo({opertion.DataItem})";

                Console.WriteLine($"  {tab}{opertion.Transaction},{instruction}\n");

            }
            if (Result == CalculationResult.Done)
            {
                Console.WriteLine("  FinalResult: All operations have been done successfully");
            }
            else
            {
                Console.WriteLine("  FinalResult: Deadlock has occurred");
            }
            Console.WriteLine("\n  /*********************************/\n");

        }
        private string CreateTab(int transactionIndex)
        {
            var result = string.Empty;
            for (var index = 0; index <= transactionIndex; index++)
            {
                result += "\t\t";
            }
            return result;
        }
        protected void InsertToWaintingList(string dataItem, string transaction)
        {
            var waiting = _waitingsList.FirstOrDefault(p => p.DataItem == dataItem);
            if (waiting == null)
            {
                waiting = new WaitingModel
                {
                    DataItem = dataItem,
                    ItemsList = new List<WaintingItemModel>()
                };
                _waitingsList.Add(waiting);
            }
            var oldWaiting = waiting.ItemsList.FirstOrDefault(c => c.Transaction == transaction);
            if (oldWaiting == null)
            {
                var lastWaiting = waiting.ItemsList.OrderByDescending(c => c.DataItemOrder).FirstOrDefault()?.DataItemOrder;
                waiting.ItemsList.Add(new WaintingItemModel
                {
                    Transaction = transaction,
                    DataItemOrder = lastWaiting != null ? lastWaiting.Value + 1 : 0
                });
            }
        }
        protected void InsertToStack(Operation operation, int scheduleCounter)
        {
            var stack = _stacksList.FirstOrDefault(c => c.Transaction == operation.Transaction);
            if (stack == null)
            {
                stack = new TransactionStackModel
                {
                    Transaction = operation.Transaction,
                    StackItemsList = new List<Operation>()
                };
                _stacksList.Add(stack);
            }
            stack.StackItemsList.Add(new Operation
            {
                DataItem = operation.DataItem,
                OperationType = operation.OperationType,
                ScheduleOrder = scheduleCounter,
                Transaction = operation.Transaction,
                TransactionOrder = operation.TransactionOrder,
                IsLockPoint = operation.IsLockPoint
            });
        }
        protected void SetLock(string dataItem, string transaction, OperationType operationType)
        {
            var checkLogResult = CheckLock(dataItem, transaction, operationType);
            if (!checkLogResult.IsLock)
            {
                /* It means upgrade Lock*/
                if (checkLogResult.LockObj != null && checkLogResult.LockObj.LockType == LockType.Shared && operationType == OperationType.Write)
                {
                    checkLogResult.LockObj.Transaction = transaction;
                    checkLogResult.LockObj.LockType = LockType.Exclusive;
                }
                else
                {
                    var lockType = LockType.Shared;
                    if (operationType == OperationType.Write)
                        lockType = LockType.Exclusive;
                    _locksList.Add(new LockModel
                    {
                        DataItem = dataItem,
                        Transaction = transaction,
                        LockType = lockType
                    });
                    ReleaseWaitingItem(dataItem, transaction);

                }

            }
        }

        protected void ReleaseWaitingItem(string dataItem, string transaction)
        {
            var waitningItem = _waitingsList.FirstOrDefault(c => c.DataItem == dataItem)?.ItemsList.FirstOrDefault(p => p.Transaction == transaction);
            if (waitningItem != null)
            {
                var waiting = _waitingsList.FirstOrDefault(c => c.DataItem == dataItem);
                waiting.ItemsList.Remove(waitningItem);
            }
        }

        protected void ReleaseLock(string dataItem, string transaction)
        {
            var lockObj = _locksList.Where(c => c.DataItem == dataItem && c.Transaction == transaction).FirstOrDefault();
            if (lockObj != null)
            {
                _locksList.Remove(lockObj);
            }
        }
        protected (bool IsLock, LockModel LockObj) CheckLock(string dataItem, string transaction, OperationType operationType)
        {
            var lockObj = _locksList.Where(c => c.DataItem == dataItem).FirstOrDefault();
            if (operationType == OperationType.Read && lockObj != null && lockObj.LockType == LockType.Exclusive && lockObj.Transaction != transaction)
                return (true, lockObj);
            else if (operationType == OperationType.Write && lockObj != null && lockObj.Transaction != transaction && lockObj.LockType== LockType.Exclusive)
                return (true, lockObj);
            else
                return (false, lockObj);
        }
        protected bool CheckIsAvailable(string dataItem, string transaction, int transactionOrder, OperationType operationType)
        {
            var checkLockResult = CheckLock(dataItem, transaction, operationType);
            if (checkLockResult.IsLock)
                return false;
            return CheckAvailability(transaction, transactionOrder);
        }
        protected bool CheckAvailability(string transaction, int transactionOrder)
        {
            var stack = _stacksList.FirstOrDefault(c => c.Transaction == transaction);
            if (stack != null && stack.StackItemsList != null && stack.StackItemsList.Count() > 0)
            {
                var lastTransactionOrder = stack.StackItemsList[stack.StackItemsList.Count - 1].TransactionOrder;
                return transactionOrder == lastTransactionOrder + 1;
            }
            return true;
        }

    }
}
