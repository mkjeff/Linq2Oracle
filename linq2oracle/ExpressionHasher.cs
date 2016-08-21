using System.Linq.Expressions;

namespace Linq2Oracle
{
    sealed class ExpressionHasher : ExpressionVisitor
    {
        static ExpressionHasher() { }

        public int Hash(Expression exp)
        {
            HashCode = 0;
            Visit(exp);
            return HashCode;
        }

        public int HashCode { get; private set; }

        ExpressionHasher Hash(int value)
        {
            unchecked { HashCode += value; }
            return this;
        }

        ExpressionHasher Hash(bool value)
        {
            unchecked { HashCode += value ? 1 : 0; }
            return this;
        }

        static readonly object s_nullValue = new object();

        ExpressionHasher Hash(object value)
        {
            value = value ?? s_nullValue;
            unchecked { HashCode += value.GetHashCode(); }
            return this;
        }

        public override Expression Visit(Expression node)
        {
            if (node == null) return node;

            Hash((int)node.NodeType).Hash(node.Type);
            return base.Visit(node);
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            Hash(node.IsLifted).Hash(node.IsLiftedToNull).Hash(node.Method);
            return base.VisitBinary(node);
        }

        protected override MemberBinding VisitMemberBinding(MemberBinding node)
        {
            Hash(node.BindingType).Hash(node.Member);
            return base.VisitMemberBinding(node);
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            Hash(node.Value);
            return base.VisitConstant(node);
        }

        protected override ElementInit VisitElementInit(ElementInit node)
        {
            Hash(node.AddMethod);
            return base.VisitElementInit(node);
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            foreach (var p in node.Parameters)
            {
                VisitParameter(p);
            }

            return base.VisitLambda(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            Hash(node.Member);
            return base.VisitMember(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            Hash(node.Method);
            return base.VisitMethodCall(node);
        }

        protected override Expression VisitNew(NewExpression node)
        {
            Hash(node.Constructor);
            if (node.Members != null)
            {
                foreach (var m in node.Members) Hash(m);
            }

            return base.VisitNew(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            Hash(node.Name);
            return base.VisitParameter(node);
        }

        protected override Expression VisitTypeBinary(TypeBinaryExpression node)
        {
            Hash(node.TypeOperand);
            return base.VisitTypeBinary(node);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            Hash(node.IsLifted).Hash(node.IsLiftedToNull).Hash(node.Method);
            return base.VisitUnary(node);
        }
    }
}
