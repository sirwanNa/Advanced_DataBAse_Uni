using System;
using System.Collections.Generic;
using System.Text;

namespace TransactionsLocking
{
    public class Operation
    {
        public string Transaction { get; set; }
        public OperationType OperationType { get; set; }
        public string DataItem { get; set; }
        public int ScheduleOrder { get; set; }
        public int TransactionOrder { get; set; }
        public bool IsLockPoint { get; set; }      
    }
    public enum OperationType
    {
        Read,
        Write,
        UnLock,
        Commit
    }
    public class LockModel
    {
        public string Transaction { get; set; }
        public string DataItem { get; set; }
        public LockType LockType { get; set; }
    }
    public class WaitingModel
    {
        public string DataItem { get; set; }
        public List<WaintingItemModel> ItemsList { get; set; }
    }
    public class WaintingItemModel
    {
        public string Transaction { get; set; }
        public int DataItemOrder { get; set; }
        // public int ScheduleOrder { get; set; }
    }
    public enum LockType
    {
        Shared,
        Exclusive
    }
    public class TransactionStackModel
    {
        public string Transaction { get; set; }
        public List<Operation> StackItemsList { get; set; }
    }
    public enum CalculationResult
    {
        Done,
        Deadlock
    }
}
