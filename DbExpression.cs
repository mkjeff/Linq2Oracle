using Oracle.ManagedDataAccess.Client;
using System;

namespace Linq2Oracle.Expressions
{
    using SqlGenerator = Action<SqlContext>;

    public interface IDbExpression
    {
        SqlGenerator Build { get; set; }
        bool IsNullExpression { get; }
    }

    public interface IDbExpression<T> : IDbExpression { }

    public interface IDbNumber : IDbExpression { }

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
                return new DbString();

            return SqlParameter.Create(value).Create<DbString>();
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
            return Operation.Binary(a, "||", b).Create<DbString>();
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
            return Function.Call("SUBSTR", this, (DbNumber)(startIndex + 1), (DbNumber)length).Create<DbString>();
        }

        public DbString Substring(DbNumber startIndex, DbNumber length)
        {
            return Function.Call("SUBSTR", this, startIndex, length).Create<DbString>();
        }

        public DbString Substring(int startIndex)
        {
            return Function.Call("SUBSTR", this, (DbNumber)(startIndex + 1)).Create<DbString>();
        }

        public DbString Substring(DbNumber startIndex)
        {
            return Function.Call("SUBSTR", this, startIndex).Create<DbString>();
        }

        public DbString Trim()
        {
            return Function.Call("TRIM", this).Create<DbString>();
        }

        public DbString TrimStart()
        {
            return Function.Call("LTRIM", this).Create<DbString>();
        }

        public DbString TrimEnd()
        {
            return Function.Call("RTRIM", this).Create<DbString>();
        }

        public DbString ToLower()
        {
            return Function.Call("LOWER", this).Create<DbString>();
        }

        public DbString ToUpper()
        {
            return Function.Call("UPPER", this).Create<DbString>();
        }
        #endregion
        #region Properties
        public DbNumber Length
        {
            get
            {
                return Function.Call("LENGTH", this).Create<DbNumber>();
            }
        }

        public DbChar this[int index]
        {
            get
            {
                return Function.Call("SUBSTR", this, (DbNumber)(index + 1), (DbNumber)1).Create<DbChar>();
            }
        }

        public DbChar this[DbNumber index]
        {
            get
            {
                return Function.Call("SUBSTR", this, index, (DbNumber)1).Create<DbChar>();
            }
        }
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression { get { return _sqlBuilder == null; } }
        #endregion
    }
    #endregion

    #region Char
    public struct DbChar : IDbExpression<char>
    {
        #region Conversion Operator
        public static implicit operator DbChar(char value)
        {
            return SqlParameter.Create(value).Create<DbChar>();
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
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression { get { return _sqlBuilder == null; } }
        #endregion
    }
    #endregion

    #region Enum
    public struct Enum<T> : IDbExpression<T> where T : struct
    {
        #region Conversion Operator
        public static implicit operator Enum<T>(T value)
        {
            return SqlParameter.Create(Enum.GetName(typeof(T), value)).Create<Enum<T>>();
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
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression { get { return _sqlBuilder == null; } }
        #endregion
    }
    #endregion

    #region DateTime
    public struct DbDateTime : IDbExpression<System.DateTime>
    {
        #region Conversion Operator
        public static implicit operator DbDateTime(System.DateTime value)
        {
            return SqlParameter.Create(value).Create<DbDateTime>();
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
            return Operation.Binary(a, "-", b).Create<DbTimeSpan>();
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

        internal NullableDbDateTime ToNullable()
        {
            return this._sqlBuilder.Create<NullableDbDateTime>();
        }
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression { get { return _sqlBuilder == null; } }
        #endregion
    }
    #endregion

    #region TimeSpan
    public struct DbTimeSpan : IDbExpression<System.TimeSpan>
    {
        #region Conversion Operator
        public static implicit operator DbTimeSpan(System.TimeSpan value)
        {
            return SqlParameter.Create(value).Create<DbTimeSpan>();
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
            return Operation.Binary(a, "+", b).Create<DbTimeSpan>();
        }

        public static DbTimeSpan operator -(DbTimeSpan a, DbTimeSpan b)
        {
            return Operation.Binary(a, "-", b).Create<DbTimeSpan>();
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

        internal NullableDbTimeSpan ToNullable()
        {
            return this._sqlBuilder.Create<NullableDbTimeSpan>();
        }
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression { get { return _sqlBuilder == null; } }
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

        static DbNumber Create<T>(T value) where T : struct
        {
            return SqlParameter.Create(value).Create<DbNumber>();
        }

        static DbNumber Create<T>(T? value) where T : struct
        {
            return SqlParameter.Create(value).Create<DbNumber>();
        }

        public static implicit operator DbNumber(short value)
        {
            return Create(value);
        }

        public static implicit operator DbNumber(int value)
        {
            return Create(value);
        }

        public static implicit operator DbNumber(long value)
        {
            return Create(value);
        }

        public static implicit operator DbNumber(float value)
        {
            return Create(value);
        }

        public static implicit operator DbNumber(double value)
        {
            return Create(value);
        }

        public static implicit operator DbNumber(decimal value)
        {
            return Create(value);
        }

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(short? value)
        {
            // C# / Visual Studio bug. if Obsolete as error, compiler will choose other version,but code editor tooltip display wrong overloaded method.
            return Create(value);
        }

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(int? value)
        {
            return Create(value);
        }

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(long? value)
        {
            return Create(value);
        }

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(float? value)
        {
            return Create(value);
        }

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(double? value)
        {
            return Create(value);
        }

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(decimal? value)
        {
            return Create(value);
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
            return Operation.Unary("+", a).Create<DbNumber>();
        }

        public static DbNumber operator -(DbNumber a)
        {
            return Operation.Unary("-", a).Create<DbNumber>();
        }

        public static DbNumber operator +(DbNumber a, DbNumber b)
        {
            return Operation.Binary(a, "+", b).Create<DbNumber>();
        }

        public static DbNumber operator -(DbNumber a, DbNumber b)
        {
            return Operation.Binary(a, "-", b).Create<DbNumber>();
        }

        public static DbNumber operator *(DbNumber a, DbNumber b)
        {
            return Operation.Binary(a, "-", b).Create<DbNumber>();
        }

        public static DbNumber operator /(DbNumber a, DbNumber b)
        {
            return Operation.Binary(a, "-", b).Create<DbNumber>();
        }

        public static DbNumber operator %(DbNumber a, DbNumber b)
        {
            return Function.Call("MOD", a, b).Create<DbNumber>();
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

        internal NullableDbNumber ToNullable()
        {
            return this._sqlBuilder.Create<NullableDbNumber>();
        }
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression { get { return _sqlBuilder == null; } }
        #endregion
    }
    #endregion

    #region Char?
    public struct NullableDbChar : IDbExpression<char?>
    {
        public DbChar GetValueOrDefault(DbChar defaultValue)
        {
            return Function.Call("NVL", this, defaultValue).Create<DbChar>();
        }

        public static implicit operator NullableDbChar(char? value)
        {
            if (!value.HasValue)
                return new NullableDbChar();

            return SqlParameter.Create(value.Value).Create<NullableDbChar>();
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
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression { get { return _sqlBuilder == null; } }
        #endregion
    }
    #endregion

    #region Enum?
    public struct NullableEnum<T> : IDbExpression<T?> where T : struct
    {
        public Enum<T> GetValueOrDefault(Enum<T> defaultValue)
        {
            return Function.Call("NVL", this, defaultValue).Create<Enum<T>>();
        }

        public static implicit operator NullableEnum<T>(T? value)
        {
            if (!value.HasValue)
                return new NullableEnum<T>();

            return SqlParameter.Create(Enum.GetName(typeof(T), value.Value)).Create<NullableEnum<T>>();
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
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression { get { return _sqlBuilder == null; } }
        #endregion
    }
    #endregion

    #region DateTime?
    public struct NullableDbDateTime : IDbExpression<DateTime?>
    {
        readonly Func<System.DateTime?> _valueProvider;

        internal NullableDbDateTime(Func<System.DateTime?> valueProvider, Action<SqlContext> sqlBuilder)
        {
            _valueProvider = valueProvider;
            _sqlBuilder = sqlBuilder;
        }

        public DbDateTime GetValueOrDefault(DbDateTime defaultValue)
        {
            return Function.Call("NVL", this, defaultValue).Create<DbDateTime>();
        }

        public static implicit operator System.DateTime?(NullableDbDateTime @this)
        {
            return @this._valueProvider();
        }

        public static implicit operator NullableDbDateTime(DateTime? value)
        {
            if (!value.HasValue)
                return new NullableDbDateTime();

            return SqlParameter.Create(value.Value).Create<NullableDbDateTime>();
        }

        public static implicit operator NullableDbDateTime(DbDateTime value)
        {
            return value.ToNullable();
        }

        public static NullableDbTimeSpan operator -(NullableDbDateTime a, NullableDbDateTime b)
        {
            return Operation.Binary(a, " - ", b).Create<NullableDbTimeSpan>();
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
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression { get { return _sqlBuilder == null; } }
        #endregion
    }
    #endregion

    #region TimeSpan?
    public struct NullableDbTimeSpan : IDbExpression<TimeSpan?>
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

        public static implicit operator NullableDbTimeSpan(TimeSpan? value)
        {
            if (!value.HasValue)
                return new NullableDbTimeSpan();

            return SqlParameter.Create(value.Value).Create<NullableDbTimeSpan>();
        }

        public static implicit operator NullableDbTimeSpan(DbTimeSpan value)
        {
            return value.ToNullable();
        }

        public DbTimeSpan GetValueOrDefault(DbTimeSpan defaultValue)
        {
            return Function.Call("NVL", this, defaultValue).Create<DbTimeSpan>();
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
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression { get { return _sqlBuilder == null; } }
        #endregion
    }
    #endregion

    #region Number?
    public struct NullableDbNumber : IDbNumber,
        IDbExpression<short?>,
        IDbExpression<int?>,
        IDbExpression<long?>,
        IDbExpression<float?>,
        IDbExpression<double?>,
        IDbExpression<decimal?>
    {
        readonly Func<decimal?> _valueProvider;

        internal NullableDbNumber(Func<decimal?> valueProvider, Action<SqlContext> sqlBuilder)
        {
            _valueProvider = valueProvider;
            _sqlBuilder = sqlBuilder;
        }

        public DbNumber GetValueOrDefault(DbNumber defaultValue)
        {
            return Function.Call("NVL", this, defaultValue).Create<DbNumber>();
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
            return value.ToNullable();
        }

        static NullableDbNumber Create<T>(T? value) where T : struct
        {
            if (!value.HasValue)
                return new NullableDbNumber();

            return SqlParameter.Create(value.Value).Create<NullableDbNumber>();
        }

        public static implicit operator NullableDbNumber(short? value)
        {
            return Create(value);
        }

        public static implicit operator NullableDbNumber(int? value)
        {
            return Create(value);
        }

        public static implicit operator NullableDbNumber(long? value)
        {
            return Create(value);
        }

        public static implicit operator NullableDbNumber(float? value)
        {
            return Create(value);
        }

        public static implicit operator NullableDbNumber(double? value)
        {
            return Create(value);
        }

        public static implicit operator NullableDbNumber(decimal? value)
        {
            return Create(value);
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
            return Operation.Unary("+", a).Create<NullableDbNumber>();
        }

        public static NullableDbNumber operator -(NullableDbNumber a)
        {
            return Operation.Unary("-", a).Create<NullableDbNumber>();
        }

        public static NullableDbNumber operator +(NullableDbNumber a, NullableDbNumber b)
        {
            return Operation.Binary(a, "+", b).Create<NullableDbNumber>();
        }

        public static NullableDbNumber operator -(NullableDbNumber a, NullableDbNumber b)
        {
            return Operation.Binary(a, "-", b).Create<NullableDbNumber>();
        }

        public static NullableDbNumber operator *(NullableDbNumber a, NullableDbNumber b)
        {
            return Operation.Binary(a, "-", b).Create<NullableDbNumber>();
        }

        public static NullableDbNumber operator /(NullableDbNumber a, NullableDbNumber b)
        {
            return Operation.Binary(a, "-", b).Create<NullableDbNumber>();
        }
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression { get { return _sqlBuilder == null; } }
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
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { } }
        bool IDbExpression.IsNullExpression { get { return false; } }
        #endregion
    }
}
