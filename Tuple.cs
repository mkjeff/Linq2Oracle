//using System;

//namespace Linq2Oracle {
//    public static class Tuple {
//        public static Tuple<T1, T2> Create<T1, T2>(T1 item1, T2 item2) {
//            return new Tuple<T1, T2>(item1, item2);
//        }
//        public static Tuple<T1, T2, T3> Create<T1, T2, T3>(T1 item1, T2 item2, T3 item3) {
//            return new Tuple<T1, T2, T3>(item1, item2, item3);
//        }
//    }

//    public sealed class Tuple<T1, T2> {
//        internal readonly T1 Item1;
//        internal readonly T2 Item2;
//        internal Tuple(T1 item1, T2 item2) {
//            this.Item1 = item1;
//            this.Item2 = item2;
//        }
//    }

//    public sealed class Tuple<T1, T2, T3> {
//        internal readonly T1 Item1;
//        internal readonly T2 Item2;
//        internal readonly T3 Item3;
//        internal Tuple(T1 item1, T2 item2, T3 item3) {
//            this.Item1 = item1;
//            this.Item2 = item2;
//            this.Item3 = item3;
//        }
//    }
//}
