using System.Linq.Expressions;

namespace Linq2Oracle
{
    sealed class ExpressionHasher : System.Linq.Expressions.ExpressionVisitor
    {
        static ExpressionHasher() { }

        public int Hash(Expression exp)
        {
            this.HashCode = 0;
            this.Visit(exp);
            return this.HashCode;
        }

        public int HashCode { get; private set; }

        ExpressionHasher Hash(int value)
        {
            unchecked { this.HashCode += value; }
            return this;
        }

        ExpressionHasher Hash(bool value)
        {
            unchecked { this.HashCode += value ? 1 : 0; }
            return this;
        }

        private static readonly object s_nullValue = new object();

        ExpressionHasher Hash(object value)
        {
            value = value ?? s_nullValue;
            unchecked { this.HashCode += value.GetHashCode(); }
            return this;
        }

        public override Expression Visit(Expression exp)
        {
            if (exp == null) return exp;

            this.Hash((int)exp.NodeType).Hash(exp.Type);
            return base.Visit(exp);
        }

        protected override Expression VisitBinary(BinaryExpression b)
        {
            this.Hash(b.IsLifted).Hash(b.IsLiftedToNull).Hash(b.Method);
            return base.VisitBinary(b);
        }

        protected override MemberBinding VisitMemberBinding(MemberBinding binding)
        {
            this.Hash(binding.BindingType).Hash(binding.Member);
            return base.VisitMemberBinding(binding);
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            this.Hash(c.Value);
            return base.VisitConstant(c);
        }

        protected override ElementInit VisitElementInit(ElementInit initializer)
        {
            this.Hash(initializer.AddMethod);
            return base.VisitElementInit(initializer);
        }

        protected override Expression VisitLambda<T>(Expression<T> lambda)
        {
            foreach (var p in lambda.Parameters)
            {
                this.VisitParameter(p);
            }

            return base.VisitLambda(lambda);
        }

        protected override Expression VisitMember(MemberExpression m)
        {
            this.Hash(m.Member);
            return base.VisitMember(m);
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            this.Hash(m.Method);
            return base.VisitMethodCall(m);
        }

        protected override Expression VisitNew(NewExpression nex)
        {
            this.Hash(nex.Constructor);
            if (nex.Members != null)
            {
                foreach (var m in nex.Members) this.Hash(m);
            }

            return base.VisitNew(nex);
        }

        protected override Expression VisitParameter(ParameterExpression p)
        {
            this.Hash(p.Name);
            return base.VisitParameter(p);
        }

        protected override Expression VisitTypeBinary(TypeBinaryExpression b)
        {
            this.Hash(b.TypeOperand);
            return base.VisitTypeBinary(b);
        }

        protected override Expression VisitUnary(UnaryExpression u)
        {
            this.Hash(u.IsLifted).Hash(u.IsLiftedToNull).Hash(u.Method);
            return base.VisitUnary(u);
        }
    }
}
