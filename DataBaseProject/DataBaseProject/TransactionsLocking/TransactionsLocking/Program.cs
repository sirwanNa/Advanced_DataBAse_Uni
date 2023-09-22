using System;

namespace TransactionsLocking
{
    class Program 
    {
        static void Main(string[] args)
        {         
            var filePath = "Sample3.txt";
            //var twoPhaseLocking = new TwoPhaseLocking();
            //twoPhaseLocking.Initiate(filePath);
            //twoPhaseLocking.Run();

            //var strictTwoPhaseLocking = new StrictTwoPhaseLocking();
            //strictTwoPhaseLocking.Initiate(filePath);
            //strictTwoPhaseLocking.Run();

            //var rigorousTwoPhaseLocking = new StrictTwoPhaseLocking();
            //rigorousTwoPhaseLocking.Initiate(filePath);
            //rigorousTwoPhaseLocking.Run();


            var timeStampLocking = new TimeStampLocking();
            timeStampLocking.Initiate(filePath);
            timeStampLocking.Run();

            Console.ReadKey();
        }
    } 
}
