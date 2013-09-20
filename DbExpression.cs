using Oracle.ManagedDataAccess.Client;
using System;

namespace Linq2Oracle.Expressions
{
    using SqlGenerator = Action<SqlContext>;

    public interface IDbExpression
    {
        void Build(SqlContext sql);
        void Setup(SqlGenerator sqlGen);
    }

    public interface IDbExpression<T> : IDbExpression { }

    public interface IDbNumber : IDbExpression { }

    public interface INullableExpression<T> : IDbExpression<T?> where T : struct { }

    #region String
    public struct DbString : IDbExpression<string>
    {
        readonly Func<string> _valueProvider;
        internal DbString(Func<string> valueProvider, Action<SqlContext> sqlBuilder)
        {
            _valueProvider = valueProvider;
            _sqlBuilder = sqlBuilder;
        }
        #region Conversion Operators
        public static implicit operator string(DbString @this)
        {
            return @this._valueProvider();
        }
        public static implicit operator DbString(string value)
        {
            if (value == null)
                return null;

            return new DbString().Init(SqlParameter.Create(value));
        }
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(DbString a, DbString b)
        {
            return a.IsEquals(b);
        }

        public static SqlBoolean operator !=(DbString a, DbString b)
        {
            return a.NotEquals(b);
        }

        public static SqlBoolean operator >(DbString a, DbString b)
        {
            return a.GreatThan(b);
        }

        public static SqlBoolean operator >=(DbString a, DbString b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static SqlBoolean operator <(DbString a, DbString b)
        {
            return a.LessThan(b);
        }

        public static SqlBoolean operator <=(DbString a, DbString b)
        {
            return a.LessThanOrEquals(b);
        }
        #endregion
        #region Custom Operator
        public static DbString operator +(DbString a, DbString b)
        {
            return new DbString().Init(Operation.Binary(a, "||", b));
        }
        #endregion
        #region Methods
        public SqlBoolean IsNullOrEmpty()
        {
            return this.IsNull();
        }

        public SqlBoolean IsNullOrWhiteSpace()
        {
            return this.TrimStart().IsNull();
        }

        public SqlBoolean StartsWith(string str)
        {
            return this.Like(str + "%");
        }

        public SqlBoolean EndsWith(string str)
        {
            return this.Like("%" + str);
        }

        public SqlBoolean Contains(string str)
        {
            return this.Like("%" + str + "%");
        }

        public SqlBoolean StartsWith(DbString str)
        {
            return this.Like(str + "%");
        }

        public SqlBoolean EndsWith(DbString str)
        {
            return this.Like("%" + str);
        }

        public SqlBoolean Contains(DbString str)
        {
            return this.Like("%" + str + "%");
        }

        public SqlBoolean Like(DbString pattern)
        {
            var @this = this;
            return new SqlBoolean(Operation.Binary(this, " LIKE ", pattern));
        }

        public SqlBoolean Equals(DbString other)
        {
            return this == other;
        }

        [Obsolete("Invalid SQL expression")]
        public new SqlBoolean Equals(object obj)
        {
            return obj is DbString ? this == (DbString)obj : new SqlBoolean();
        }

        public DbString Substring(int startIndex, int length)
        {
            return new DbString().Init(Function.Call("SUBSTR", this, (DbNumber)(startIndex + 1), (DbNumber)length));
        }

        public DbString Substring(DbNumber startIndex, DbNumber length)
        {
            return new DbString().Init(Function.Call("SUBSTR", this, startIndex, length));
        }

        public DbString Substring(int startIndex)
        {
            return new DbString().Init(Function.Call("SUBSTR", this, (DbNumber)(startIndex + 1)));
        }

        public DbString Substring(DbNumber startIndex)
        {
            return new DbString().Init(Function.Call("SUBSTR", this, startIndex));
        }

        public DbString Trim()
        {
            return new DbString().Init(Function.Call("TRIM", this));
        }

        public DbString TrimStart()
        {
            return new DbString().Init(Function.Call("LTRIM", this));
        }

        public DbString TrimEnd()
        {
            return new DbString().Init(Function.Call("RTRIM", this));
        }

        public DbString ToLower()
        {
            return new DbString().Init(Function.Call("LOWER", this));
        }

        public DbString ToUpper()
        {
            return new DbString().Init(Function.Call("UPPER", this));
        }
        #endregion
        #region Properties
        public DbNumber Length
        {
            get
            {
                return new DbNumber().Init(Function.Call("LENGTH", this));
            }
        }

        public DbChar this[int index]
        {
            get
            {
                return new DbChar().Init(Function.Call("SUBSTR", this, (DbNumber)(index + 1), (DbNumber)1));
            }
        }

        public DbChar this[DbNumber index]
        {
            get
            {
                return new DbChar().Init(Function.Call("SUBSTR", this, index, (DbNumber)1));
            }
        }
        #endregion
        #region IDbExpression
        void IDbExpression.Build(SqlContext sql)
        {
            _sqlBuilder(sql);
        }

        SqlGenerator _sqlBuilder;

        void IDbExpression.Setup(SqlGenerator sqlGen)
        {
            _sqlBuilder = sqlGen;
        }
        #endregion
    }
    #endregion

    #region Char
    public struct DbChar : IDbExpression<char>
    {
        #region Conversion Operator
        public static implicit operator DbChar(char value)
        {
            return new DbChar().Init(SqlParameter.Create(value));
        }
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(DbChar a, DbChar b)
        {
            return a.IsEquals(b);
        }

        public static SqlBoolean operator !=(DbChar a, DbChar b)
        {
            return a.NotEquals(b);
        }

        public static SqlBoolean operator >(DbChar a, DbChar b)
        {
            return a.GreatThan(b);
        }

        public static SqlBoolean operator >=(DbChar a, DbChar b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static SqlBoolean operator <(DbChar a, DbChar b)
        {
            return a.LessThan(b);
        }

        public static SqlBoolean operator <=(DbChar a, DbChar b)
        {
            return a.LessThanOrEquals(b);
        }
        #endregion
        #region Methods
        public SqlBoolean Equals(DbChar other)
        {
            return this == other;
        }

        [Obsolete("Invalid SQL expression")]
        public new SqlBoolean Equals(object obj)
        {
            return obj is DbChar ? this == (DbChar)obj : new SqlBoolean();
        }
        #endregion
        #region IDbExpression
        void IDbExpression.Build(SqlContext sql)
        {
            _sqlBuilder(sql);
        }

        SqlGenerator _sqlBuilder;

        void IDbExpression.Setup(SqlGenerator sqlGen)
        {
            _sqlBuilder = sqlGen;
        }
        #endregion
    }
    #endregion

    #region Enum
    public struct Enum<T> : IDbExpression<T> where T : struct
    {
        #region Conversion Operator
        public static implicit operator Enum<T>(T value)
        {
            return new Enum<T>().Init(SqlParameter.Create(value, OracleDbType.Varchar2));
        }
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(Enum<T> a, Enum<T> b)
        {
            return a.IsEquals(b);
        }

        public static SqlBoolean operator !=(Enum<T> a, Enum<T> b)
        {
            return a.NotEquals(b);
        }

        //public static SqlBoolean operator >(Enum<T> a, Enum<T> b)
        //{
        //    return a.GreatThan(b);
        //}

        //public static SqlBoolean operator >=(Enum<T> a, Enum<T> b)
        //{
        //    return a.GreatThanOrEquals(b);
        //}

        //public static SqlBoolean operator <(Enum<T> a, Enum<T> b)
        //{
        //    return a.LessThan(b);
        //}

        //public static SqlBoolean operator <=(Enum<T> a, Enum<T> b)
        //{
        //    return a.LessThanOrEquals(b);
        //}
        #endregion
        #region Methods
        public SqlBoolean Equals(Enum<T> other)
        {
            return this == other;
        }

        [Obsolete("Invalid SQL expression")]
        public new SqlBoolean Equals(object obj)
        {
            return obj is Enum<T> ? this == (Enum<T>)obj : new SqlBoolean();
        }
        #endregion
        #region IDbExpression
        void IDbExpression.Build(SqlContext sql)
        {
            _sqlBuilder(sql);
        }

        SqlGenerator _sqlBuilder;

        void IDbExpression.Setup(SqlGenerator sqlGen)
        {
            _sqlBuilder = sqlGen;
        }
        #endregion
    }
    #endregion

    #region DateTime
    public struct DbDateTime : IDbExpression<System.DateTime>
    {
        #region Conversion Operator
        public static implicit operator DbDateTime(System.DateTime value)
        {
            return new DbDateTime().Init(SqlParameter.Create(value));
        }
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(DbDateTime a, DbDateTime b)
        {
            return a.IsEquals(b);
        }

        public static SqlBoolean operator !=(DbDateTime a, DbDateTime b)
        {
            return a.NotEquals(b);
        }

        public static SqlBoolean operator >(DbDateTime a, DbDateTime b)
        {
            return a.GreatThan(b);
        }

        public static SqlBoolean operator >=(DbDateTime a, DbDateTime b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static SqlBoolean operator <(DbDateTime a, DbDateTime b)
        {
            return a.LessThan(b);
        }

        public static SqlBoolean operator <=(DbDateTime a, DbDateTime b)
        {
            return a.LessThanOrEquals(b);
        }
        #endregion
        #region Custom Operator
        public static DbTimeSpan operator -(DbDateTime a, DbDateTime b)
        {
            return new DbTimeSpan().Init(Operation.Binary(a, "-", b));
        }
        #endregion
        #region Methods
        public SqlBoolean Equals(DbDateTime other)
        {
            return this == other;
        }

        [Obsolete("Invalid SQL expression")]
        public new SqlBoolean Equals(object obj)
        {
            return obj is DbDateTime ? this == (DbDateTime)obj : new SqlBoolean();
        }
        #endregion
        #region IDbExpression
        void IDbExpression.Build(SqlContext sql)
        {
            _sqlBuilder(sql);
        }

        SqlGenerator _sqlBuilder;

        void IDbExpression.Setup(SqlGenerator sqlGen)
        {
            _sqlBuilder = sqlGen;
        }
        #endregion
    }
    #endregion

    #region TimeSpan
    public struct DbTimeSpan : IDbExpression<System.TimeSpan>
    {
        #region Conversion Operator
        public static implicit operator DbTimeSpan(System.TimeSpan value)
        {
            return new DbTimeSpan().Init(SqlParameter.Create(value));
        }
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(DbTimeSpan a, DbTimeSpan b)
        {
            return a.IsEquals(b);
        }

        public static SqlBoolean operator !=(DbTimeSpan a, DbTimeSpan b)
        {
            return a.NotEquals(b);
        }

        public static SqlBoolean operator >(DbTimeSpan a, DbTimeSpan b)
        {
            return a.GreatThan(b);
        }

        public static SqlBoolean operator >=(DbTimeSpan a, DbTimeSpan b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static SqlBoolean operator <(DbTimeSpan a, DbTimeSpan b)
        {
            return a.LessThan(b);
        }

        public static SqlBoolean operator <=(DbTimeSpan a, DbTimeSpan b)
        {
            return a.LessThanOrEquals(b);
        }
        #endregion
        #region Custom Operator
        //[Obsolete("Oracle IntervalDS don't support.")]
        //public static Decimal operator +(Decimal a)
        //{
        //    return new Decimal().Init(OracleDbType.Decimal, Operation.Unary("+", a));
        //}

        //[Obsolete("Oracle IntervalDS don't support.")]
        //public static Decimal operator -(Decimal a)
        //{
        //    return new Decimal().Init(OracleDbType.Decimal, Operation.Unary("-", a));
        //}

        public static DbTimeSpan operator +(DbTimeSpan a, DbTimeSpan b)
        {
            return new DbTimeSpan().Init(Operation.Binary(a, "+", b));
        }

        public static DbTimeSpan operator -(DbTimeSpan a, DbTimeSpan b)
        {
            return new DbTimeSpan().Init(Operation.Binary(a, "-", b));
        }
        #endregion
        #region Methods
        public SqlBoolean Equals(DbTimeSpan other)
        {
            return this == other;
        }

        [Obsolete("Invalid SQL expression")]
        public new SqlBoolean Equals(object obj)
        {
            return obj is DbTimeSpan ? this == (DbTimeSpan)obj : new SqlBoolean();
        }
        #endregion
        #region IDbExpression
        void IDbExpression.Build(SqlContext sql)
        {
            _sqlBuilder(sql);
        }

        SqlGenerator _sqlBuilder;

        void IDbExpression.Setup(SqlGenerator sqlGen)
        {
            _sqlBuilder = sqlGen;
        }
        #endregion
    }
    #endregion

    #region Number
    public struct DbNumber : IDbNumber,
        IDbExpression<short>,
        IDbExpression<int>,
        IDbExpression<long>,
        IDbExpression<float>,
        IDbExpression<double>,
        IDbExpression<decimal>
    {
        readonly Func<decimal> _valueProvider;
        internal DbNumber(Func<decimal> valueProvider, Action<SqlContext> sqlBuilder)
        {
            _valueProvider = valueProvider;
            _sqlBuilder = sqlBuilder;
        }
        #region Conversion Operator

        public static implicit operator short(DbNumber @this)
        {
            return (short)@this._valueProvider();
        }

        public static implicit operator int(DbNumber @this)
        {
            return (int)@this._valueProvider();
        }

        public static implicit operator long(DbNumber @this)
        {
            return (long)@this._valueProvider();
        }

        public static implicit operator float(DbNumber @this)
        {
            return (float)@this._valueProvider();
        }

        public static implicit operator double(DbNumber @this)
        {
            return (double)@this._valueProvider();
        }

        public static implicit operator decimal(DbNumber @this)
        {
            return @this._valueProvider();
        }

        public static implicit operator DbNumber(short value)
        {
            return new DbNumber().Init(SqlParameter.Create(value));
        }

        public static implicit operator DbNumber(int value)
        {
            return new DbNumber().Init(SqlParameter.Create(value));
        }

        public static implicit operator DbNumber(long value)
        {
            return new DbNumber().Init(SqlParameter.Create(value));
        }

        public static implicit operator DbNumber(float value)
        {
            return new DbNumber().Init(SqlParameter.Create(value));
        }

        public static implicit operator DbNumber(double value)
        {
            return new DbNumber().Init(SqlParameter.Create(value));
        }

        public static implicit operator DbNumber(decimal value)
        {
            return new DbNumber().Init(SqlParameter.Create(value));
        }

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(short? value)
        {
            // C# / Visual Studio bug. if Obsolete as error, compiler will choose other version,but code editor tooltip display wrong overloaded method.
            return new DbNumber().Init(SqlParameter.Create(value));
        }

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(int? value)
        {
            return new DbNumber().Init(SqlParameter.Create(value));
        }

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(long? value)
        {
            return new DbNumber().Init(SqlParameter.Create(value));
        }

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(float? value)
        {
            return new DbNumber().Init(SqlParameter.Create(value));
        }

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(double? value)
        {
            return new DbNumber().Init(SqlParameter.Create(value));
        }

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(decimal? value)
        {
            return new DbNumber().Init(SqlParameter.Create(value));
        }
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(DbNumber a, DbNumber b)
        {
            return a.IsEquals(b);
        }

        public static SqlBoolean operator !=(DbNumber a, DbNumber b)
        {
            return a.NotEquals(b);
        }

        public static SqlBoolean operator >(DbNumber a, DbNumber b)
        {
            return a.GreatThan(b);
        }

        public static SqlBoolean operator >=(DbNumber a, DbNumber b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static SqlBoolean operator <(DbNumber a, DbNumber b)
        {
            return a.LessThan(b);
        }

        public static SqlBoolean operator <=(DbNumber a, DbNumber b)
        {
            return a.LessThanOrEquals(b);
        }
        #endregion
        #region Custom Operator
        public static DbNumber operator +(DbNumber a)
        {
            return new DbNumber().Init(Operation.Unary("+", a));
        }

        public static DbNumber operator -(DbNumber a)
        {
            return new DbNumber().Init(Operation.Unary("-", a));
        }

        public static DbNumber operator +(DbNumber a, DbNumber b)
        {
            return new DbNumber().Init(Operation.Binary(a, "+", b));
        }

        public static DbNumber operator -(DbNumber a, DbNumber b)
        {
            return new DbNumber().Init(Operation.Binary(a, "-", b));
        }

        public static DbNumber operator *(DbNumber a, DbNumber b)
        {
            return new DbNumber().Init(Operation.Binary(a, "-", b));
        }

        public static DbNumber operator /(DbNumber a, DbNumber b)
        {
            return new DbNumber().Init(Operation.Binary(a, "-", b));
        }

        public static DbNumber operator %(DbNumber a, DbNumber b)
        {
            return new DbNumber().Init(Function.Call("MOD", a, b));
        }
        #endregion
        #region Methods
        public SqlBoolean Equals(DbNumber other)
        {
            return this == other;
        }

        [Obsolete("Invalid SQL expression")]
        public new SqlBoolean Equals(object obj)
        {
            return obj is DbNumber ? this == (DbNumber)obj : new SqlBoolean();
        }
        #endregion
        #region IDbExpression
        void IDbExpression.Build(SqlContext sql)
        {
            _sqlBuilder(sql);
        }

        SqlGenerator _sqlBuilder;

        void IDbExpression.Setup(SqlGenerator sqlGen)
        {
            _sqlBuilder = sqlGen;
        }
        #endregion
    }
    #endregion

    #region Char?
    public struct NullableDbChar : INullableExpression<char>
    {
        public DbChar GetValueOrDefault(DbChar defaultValue)
        {
            return new DbChar().Init(Function.Call("NVL", this, defaultValue));
        }

        public static implicit operator NullableDbChar(char? value)
        {
            if (!value.HasValue)
                return null;
            return new NullableDbChar().Init(sql => sql.AppendParam(value));
        }

        #region Comparision Operators
        public static SqlBoolean operator ==(NullableDbChar a, NullableDbChar b)
        {
            return a.IsEquals(b);
        }

        public static SqlBoolean operator !=(NullableDbChar a, NullableDbChar b)
        {
            return a.NotEquals(b);
        }

        public static SqlBoolean operator >(NullableDbChar a, NullableDbChar b)
        {
            return a.GreatThan(b);
        }

        public static SqlBoolean operator >=(NullableDbChar a, NullableDbChar b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static SqlBoolean operator <(NullableDbChar a, NullableDbChar b)
        {
            return a.LessThan(b);
        }

        public static SqlBoolean operator <=(NullableDbChar a, NullableDbChar b)
        {
            return a.LessThanOrEquals(b);
        }
        #endregion
        #region IDbExpression
        void IDbExpression.Build(SqlContext sql)
        {
            _sqlBuilder(sql);
        }

        SqlGenerator _sqlBuilder;

        void IDbExpression.Setup(SqlGenerator sqlGen)
        {
            _sqlBuilder = sqlGen;
        }
        #endregion
    }
    #endregion

    #region Enum?
    public struct NullableEnum<T> :  INullableExpression<T> where T : struct
    {
        public Enum<T> GetValueOrDefault(Enum<T> defaultValue)
        {
            return new Enum<T>().Init(Function.Call("NVL", this, defaultValue));
        }

        public static implicit operator NullableEnum<T>(T? value)
        {
            if (!value.HasValue)
                return null;
            return new NullableEnum<T>().Init(sql => sql.AppendParam(OracleDbType.Varchar2, value.Value));
        }
        #region Comparision Operators
        public static SqlBoolean operator ==(NullableEnum<T> a, NullableEnum<T> b)
        {
            return a.IsEquals(b);
        }

        public static SqlBoolean operator !=(NullableEnum<T> a, NullableEnum<T> b)
        {
            return a.NotEquals(b);
        }

        public static SqlBoolean operator >(NullableEnum<T> a, NullableEnum<T> b)
        {
            return a.GreatThan(b);
        }

        public static SqlBoolean operator >=(NullableEnum<T> a, NullableEnum<T> b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static SqlBoolean operator <(NullableEnum<T> a, NullableEnum<T> b)
        {
            return a.LessThan(b);
        }

        public static SqlBoolean operator <=(NullableEnum<T> a, NullableEnum<T> b)
        {
            return a.LessThanOrEquals(b);
        }
        #endregion
        #region IDbExpression
        void IDbExpression.Build(SqlContext sql)
        {
            _sqlBuilder(sql);
        }

        SqlGenerator _sqlBuilder;

        void IDbExpression.Setup(SqlGenerator sqlGen)
        {
            _sqlBuilder = sqlGen;
        }
        #endregion
    }
    #endregion

    #region DateTime?
    public struct NullableDbDateTime : INullableExpression<System.DateTime>
    {
        readonly Func<System.DateTime?> _valueProvider;

        internal NullableDbDateTime(Func<System.DateTime?> valueProvider, Action<SqlContext> sqlBuilder)
        {
            _valueProvider = valueProvider;
            _sqlBuilder = sqlBuilder;
        }

        public DbDateTime GetValueOrDefault(DbDateTime defaultValue)
        {
            return new DbDateTime().Init(Function.Call("NVL", this, defaultValue));
        }

        public static implicit operator System.DateTime?(NullableDbDateTime @this)
        {
            return @this._valueProvider();
        }

        public static implicit operator NullableDbDateTime(DbDateTime? value)
        {
            if (!value.HasValue)
                return null;
            return new NullableDbDateTime().Init(sql => sql.AppendParam(value.Value));
        }

        public static NullableDbTimeSpan operator -(NullableDbDateTime a, NullableDbDateTime b)
        {
            return new NullableDbTimeSpan().Init(Operation.Binary(a, " - ", b));
        }

        #region Comparision Operators
        public static SqlBoolean operator ==(NullableDbDateTime a, NullableDbDateTime b)
        {
            return a.IsEquals(b);
        }

        public static SqlBoolean operator !=(NullableDbDateTime a, NullableDbDateTime b)
        {
            return a.NotEquals(b);
        }

        public static SqlBoolean operator >(NullableDbDateTime a, NullableDbDateTime b)
        {
            return a.GreatThan(b);
        }

        public static SqlBoolean operator >=(NullableDbDateTime a, NullableDbDateTime b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static SqlBoolean operator <(NullableDbDateTime a, NullableDbDateTime b)
        {
            return a.LessThan(b);
        }

        public static SqlBoolean operator <=(NullableDbDateTime a, NullableDbDateTime b)
        {
            return a.LessThanOrEquals(b);
        }
        #endregion
        #region IDbExpression
        void IDbExpression.Build(SqlContext sql)
        {
            _sqlBuilder(sql);
        }

        SqlGenerator _sqlBuilder;

        void IDbExpression.Setup(SqlGenerator sqlGen)
        {
            _sqlBuilder = sqlGen;
        }
        #endregion
    }
    #endregion

    #region TimeSpan?
    public struct NullableDbTimeSpan : INullableExpression<System.TimeSpan>
    {
        readonly Func<System.TimeSpan?> _valueProvider;

        internal NullableDbTimeSpan(Func<System.TimeSpan?> valueProvider, Action<SqlContext> sqlBuilder)
        {
            _valueProvider = valueProvider;
            _sqlBuilder = sqlBuilder;
        }

        public static implicit operator System.TimeSpan?(NullableDbTimeSpan @this)
        {
            return @this._valueProvider();
        }

        public static implicit operator NullableDbTimeSpan(DbTimeSpan? value)
        {
            if (!value.HasValue)
                return null;
            return new NullableDbTimeSpan().Init(sql => sql.AppendParam(value.Value));
        }

        public DbTimeSpan GetValueOrDefault(DbTimeSpan defaultValue)
        {
            return new DbTimeSpan().Init(Function.Call("NVL", this, defaultValue));
        }

        #region Comparision Operators
        public static SqlBoolean operator ==(NullableDbTimeSpan a, NullableDbTimeSpan b)
        {
            return a.IsEquals(b);
        }

        public static SqlBoolean operator !=(NullableDbTimeSpan a, NullableDbTimeSpan b)
        {
            return a.NotEquals(b);
        }

        public static SqlBoolean operator >(NullableDbTimeSpan a, NullableDbTimeSpan b)
        {
            return a.GreatThan(b);
        }

        public static SqlBoolean operator >=(NullableDbTimeSpan a, NullableDbTimeSpan b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static SqlBoolean operator <(NullableDbTimeSpan a, NullableDbTimeSpan b)
        {
            return a.LessThan(b);
        }

        public static SqlBoolean operator <=(NullableDbTimeSpan a, NullableDbTimeSpan b)
        {
            return a.LessThanOrEquals(b);
        }
        #endregion
        #region IDbExpression
        void IDbExpression.Build(SqlContext sql)
        {
            _sqlBuilder(sql);
        }

        SqlGenerator _sqlBuilder;

        void IDbExpression.Setup(SqlGenerator sqlGen)
        {
            _sqlBuilder = sqlGen;
        }
        #endregion
    }
    #endregion

    #region Number?
    public struct NullableDbNumber : IDbNumber,
        INullableExpression<short>,
        INullableExpression<int>,
        INullableExpression<long>,
        INullableExpression<float>,
        INullableExpression<double>,
        INullableExpression<decimal>
    {
        readonly Func<decimal?> _valueProvider;

        internal NullableDbNumber(Func<decimal?> valueProvider, Action<SqlContext> sqlBuilder)
        {
            _valueProvider = valueProvider;
            _sqlBuilder = sqlBuilder;
        }

        public DbNumber GetValueOrDefault(DbNumber defaultValue)
        {
            return new DbNumber().Init(Function.Call("NVL", this, defaultValue));
        }

        #region Conversion Operator
        public static implicit operator short?(NullableDbNumber @this)
        {
            return (short?)@this._valueProvider();
        }

        public static implicit operator int?(NullableDbNumber @this)
        {
            return (int?)@this._valueProvider();
        }

        public static implicit operator long?(NullableDbNumber @this)
        {
            return (long?)@this._valueProvider();
        }

        public static implicit operator float?(NullableDbNumber @this)
        {
            return (float?)@this._valueProvider();
        }

        public static implicit operator double?(NullableDbNumber @this)
        {
            return (double?)@this._valueProvider();
        }

        public static implicit operator decimal?(NullableDbNumber @this)
        {
            return @this._valueProvider();
        }

        public static implicit operator NullableDbNumber(DbNumber value)
        {
            return new NullableDbNumber().Init(sql => sql.Append(value));
        }

        public static implicit operator NullableDbNumber(short? value)
        {
            if (!value.HasValue)
                return null;
            return new NullableDbNumber().Init(sql => sql.AppendParam(value.Value));
        }

        public static implicit operator NullableDbNumber(int? value)
        {
            if (!value.HasValue)
                return null;
            return new NullableDbNumber().Init(sql => sql.AppendParam(value.Value));
        }

        public static implicit operator NullableDbNumber(long? value)
        {
            if (!value.HasValue)
                return null;
            return new NullableDbNumber().Init(sql => sql.AppendParam(value.Value));
        }

        public static implicit operator NullableDbNumber(float? value)
        {
            if (!value.HasValue)
                return null;
            return new NullableDbNumber().Init(sql => sql.AppendParam(value.Value));
        }

        public static implicit operator NullableDbNumber(double? value)
        {
            if (!value.HasValue)
                return null;
            return new NullableDbNumber().Init(sql => sql.AppendParam(value.Value));
        }

        public static implicit operator NullableDbNumber(decimal? value)
        {
            if (!value.HasValue)
                return null;
            return new NullableDbNumber().Init(sql => sql.AppendParam(value.Value));
        }
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(NullableDbNumber a, NullableDbNumber b)
        {
            return a.IsEquals(b);
        }

        public static SqlBoolean operator !=(NullableDbNumber a, NullableDbNumber b)
        {
            return a.NotEquals(b);
        }

        public static SqlBoolean operator >(NullableDbNumber a, NullableDbNumber b)
        {
            return a.GreatThan(b);
        }

        public static SqlBoolean operator >=(NullableDbNumber a, NullableDbNumber b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static SqlBoolean operator <(NullableDbNumber a, NullableDbNumber b)
        {
            return a.LessThan(b);
        }

        public static SqlBoolean operator <=(NullableDbNumber a, NullableDbNumber b)
        {
            return a.LessThanOrEquals(b);
        }
        #endregion
        #region Custom Operator
        public static NullableDbNumber operator +(NullableDbNumber a)
        {
            return new NullableDbNumber().Init(Operation.Unary("+", a));
        }

        public static NullableDbNumber operator -(NullableDbNumber a)
        {
            return new NullableDbNumber().Init(Operation.Unary("-", a));
        }

        public static NullableDbNumber operator +(NullableDbNumber a, NullableDbNumber b)
        {
            return new NullableDbNumber().Init(Operation.Binary(a, "+", b));
        }

        public static NullableDbNumber operator -(NullableDbNumber a, NullableDbNumber b)
        {
            return new NullableDbNumber().Init(Operation.Binary(a, "-", b));
        }

        public static NullableDbNumber operator *(NullableDbNumber a, NullableDbNumber b)
        {
            return new NullableDbNumber().Init(Operation.Binary(a, "-", b));
        }

        public static NullableDbNumber operator /(NullableDbNumber a, NullableDbNumber b)
        {
            return new NullableDbNumber().Init(Operation.Binary(a, "-", b));
        }
        #endregion
        #region IDbExpression
        void IDbExpression.Build(SqlContext sql)
        {
            _sqlBuilder(sql);
        }

        SqlGenerator _sqlBuilder;

        void IDbExpression.Setup(SqlGenerator sqlGen)
        {
            _sqlBuilder = sqlGen;
        }
        #endregion
    }
    #endregion

    sealed class ColumnExpression : IDbExpression
    {
        readonly SqlGenerator _sqlBuilder;

        internal ColumnExpression(IQueryContext table, string columnName)
        {
            _sqlBuilder = sql => sql.Append(sql.GetAlias(table)).Append(".").Append(columnName);
        }

        #region IDbExpression
        void IDbExpression.Build(SqlContext sql)
        {
            _sqlBuilder(sql);
        }

        void IDbExpression.Setup(SqlGenerator sqlGen)
        {
        }
        #endregion
    }
}
