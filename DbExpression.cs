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
        public static implicit operator string(DbString @this) => @this._valueProvider();

        public static implicit operator DbString(string value) => value == null ? new DbString() : SqlParameter.Create(value).Create<DbString>();
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(DbString a, DbString b) => a.IsEquals(b);

        public static SqlBoolean operator !=(DbString a, DbString b) => a.NotEquals(b);

        public static SqlBoolean operator >(DbString a, DbString b) => a.GreatThan(b);

        public static SqlBoolean operator >=(DbString a, DbString b) => a.GreatThanOrEquals(b);

        public static SqlBoolean operator <(DbString a, DbString b) => a.LessThan(b);

        public static SqlBoolean operator <=(DbString a, DbString b) => a.LessThanOrEquals(b);
        #endregion
        #region Custom Operator
        public static DbString operator +(DbString a, DbString b) => Operatior.Binary(a, "||", b).Create<DbString>();
        #endregion
        #region Methods
        public SqlBoolean IsNullOrEmpty() => this.IsNull();

        public SqlBoolean IsNullOrWhiteSpace() => this.TrimStart().IsNull();

        public SqlBoolean StartsWith(string str) => this.Like(str + "%");

        public SqlBoolean EndsWith(string str) => this.Like("%" + str);

        public SqlBoolean Contains(string str) => this.Like("%" + str + "%");

        public SqlBoolean StartsWith(DbString str) => this.Like(str + "%");

        public SqlBoolean EndsWith(DbString str) => this.Like("%" + str);

        public SqlBoolean Contains(DbString str) => this.Like("%" + str + "%");

        public SqlBoolean Like(DbString pattern) => new SqlBoolean(Operatior.Binary(this, " LIKE ", pattern));

        public SqlBoolean Equals(DbString other) => this == other;

        [Obsolete("Invalid SQL expression")]
        public new SqlBoolean Equals(object obj) => obj is DbString ? this == (DbString)obj : new SqlBoolean();

        public DbString Substring(int startIndex, int length) => Function.Call("SUBSTR", this, (DbNumber)(startIndex + 1), (DbNumber)length).Create<DbString>();

        public DbString Substring(DbNumber startIndex, DbNumber length) => Function.Call("SUBSTR", this, startIndex, length).Create<DbString>();

        public DbString Substring(int startIndex) => Function.Call("SUBSTR", this, (DbNumber)(startIndex + 1)).Create<DbString>();

        public DbString Substring(DbNumber startIndex) => Function.Call("SUBSTR", this, startIndex).Create<DbString>();

        public DbString Trim() => Function.Call("TRIM", this).Create<DbString>();

        public DbString TrimStart() => Function.Call("LTRIM", this).Create<DbString>();

        public DbString TrimEnd() => Function.Call("RTRIM", this).Create<DbString>();

        public DbString ToLower() => Function.Call("LOWER", this).Create<DbString>();

        public DbString ToUpper() => Function.Call("UPPER", this).Create<DbString>();
        #endregion
        #region Properties
        public DbNumber Length => Function.Call("LENGTH", this).Create<DbNumber>();

        public DbChar this[int index] => Function.Call("SUBSTR", this, (DbNumber)(index + 1), (DbNumber)1).Create<DbChar>();

        public DbChar this[DbNumber index] => Function.Call("SUBSTR", this, index, (DbNumber)1).Create<DbChar>();
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression => _sqlBuilder == null;
        #endregion
    }
    #endregion

    #region Char
    public struct DbChar : IDbExpression<char>
    {
        #region Conversion Operator
        public static implicit operator DbChar(char value) => SqlParameter.Create(value).Create<DbChar>();
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(DbChar a, DbChar b) => a.IsEquals(b);

        public static SqlBoolean operator !=(DbChar a, DbChar b) => a.NotEquals(b);

        public static SqlBoolean operator >(DbChar a, DbChar b) => a.GreatThan(b);

        public static SqlBoolean operator >=(DbChar a, DbChar b) => a.GreatThanOrEquals(b);

        public static SqlBoolean operator <(DbChar a, DbChar b) => a.LessThan(b);

        public static SqlBoolean operator <=(DbChar a, DbChar b) => a.LessThanOrEquals(b);
        #endregion
        #region Methods
        public SqlBoolean Equals(DbChar other) => this == other;

        [Obsolete("Invalid SQL expression")]
        public new SqlBoolean Equals(object obj) => obj is DbChar ? this == (DbChar)obj : new SqlBoolean();
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression => _sqlBuilder == null;
        #endregion
    }
    #endregion

    #region Enum
    public struct Enum<T> : IDbExpression<T> where T : struct
    {
        #region Conversion Operator
        public static implicit operator Enum<T>(T value) => SqlParameter.Create(Enum.GetName(typeof(T), value)).Create<Enum<T>>();
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(Enum<T> a, Enum<T> b) => a.IsEquals(b);

        public static SqlBoolean operator !=(Enum<T> a, Enum<T> b) => a.NotEquals(b);

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
        public SqlBoolean Equals(Enum<T> other) => this == other;

        [Obsolete("Invalid SQL expression")]
        public new SqlBoolean Equals(object obj) => obj is Enum<T> ? this == (Enum<T>)obj : new SqlBoolean();
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression => _sqlBuilder == null;
        #endregion
    }
    #endregion

    #region DateTime
    public struct DbDateTime : IDbExpression<System.DateTime>
    {
        #region Conversion Operator
        public static implicit operator DbDateTime(System.DateTime value) => SqlParameter.Create(value).Create<DbDateTime>();
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(DbDateTime a, DbDateTime b) => a.IsEquals(b);

        public static SqlBoolean operator !=(DbDateTime a, DbDateTime b) => a.NotEquals(b);

        public static SqlBoolean operator >(DbDateTime a, DbDateTime b) => a.GreatThan(b);

        public static SqlBoolean operator >=(DbDateTime a, DbDateTime b) => a.GreatThanOrEquals(b);

        public static SqlBoolean operator <(DbDateTime a, DbDateTime b) => a.LessThan(b);

        public static SqlBoolean operator <=(DbDateTime a, DbDateTime b) => a.LessThanOrEquals(b);
        #endregion
        #region Custom Operator
        public static DbTimeSpan operator -(DbDateTime a, DbDateTime b) => Operatior.Binary(a, "-", b).Create<DbTimeSpan>();
        #endregion
        #region Methods
        public SqlBoolean Equals(DbDateTime other) => this == other;

        [Obsolete("Invalid SQL expression")]
        public new SqlBoolean Equals(object obj) => obj is DbDateTime ? this == (DbDateTime)obj : new SqlBoolean();

        internal NullableDbDateTime ToNullable() => this._sqlBuilder.Create<NullableDbDateTime>();
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression => _sqlBuilder == null;
        #endregion
    }
    #endregion

    #region TimeSpan
    public struct DbTimeSpan : IDbExpression<System.TimeSpan>
    {
        #region Conversion Operator
        public static implicit operator DbTimeSpan(System.TimeSpan value) => SqlParameter.Create(value).Create<DbTimeSpan>();
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(DbTimeSpan a, DbTimeSpan b) => a.IsEquals(b);

        public static SqlBoolean operator !=(DbTimeSpan a, DbTimeSpan b) => a.NotEquals(b);

        public static SqlBoolean operator >(DbTimeSpan a, DbTimeSpan b) => a.GreatThan(b);

        public static SqlBoolean operator >=(DbTimeSpan a, DbTimeSpan b) => a.GreatThanOrEquals(b);

        public static SqlBoolean operator <(DbTimeSpan a, DbTimeSpan b) => a.LessThan(b);

        public static SqlBoolean operator <=(DbTimeSpan a, DbTimeSpan b) => a.LessThanOrEquals(b);
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

        public static DbTimeSpan operator +(DbTimeSpan a, DbTimeSpan b) => Operatior.Binary(a, "+", b).Create<DbTimeSpan>();

        public static DbTimeSpan operator -(DbTimeSpan a, DbTimeSpan b) => Operatior.Binary(a, "-", b).Create<DbTimeSpan>();
        #endregion
        #region Methods
        public SqlBoolean Equals(DbTimeSpan other) => this == other;

        [Obsolete("Invalid SQL expression")]
        public new SqlBoolean Equals(object obj) => obj is DbTimeSpan ? this == (DbTimeSpan)obj : new SqlBoolean();

        internal NullableDbTimeSpan ToNullable() => this._sqlBuilder.Create<NullableDbTimeSpan>();
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression => _sqlBuilder == null;
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

        public static implicit operator short(DbNumber @this) => (short)@this._valueProvider();

        public static implicit operator int(DbNumber @this) => (int)@this._valueProvider();

        public static implicit operator long(DbNumber @this) => (long)@this._valueProvider();

        public static implicit operator float(DbNumber @this) => (float)@this._valueProvider();

        public static implicit operator double(DbNumber @this) => (double)@this._valueProvider();

        public static implicit operator decimal(DbNumber @this) => @this._valueProvider();

        static DbNumber Create<T>(T value) where T : struct => SqlParameter.Create(value).Create<DbNumber>();

        static DbNumber Create<T>(T? value) where T : struct => SqlParameter.Create(value).Create<DbNumber>();

        public static implicit operator DbNumber(short value) => Create(value);

        public static implicit operator DbNumber(int value) => Create(value);

        public static implicit operator DbNumber(long value) => Create(value);

        public static implicit operator DbNumber(float value) => Create(value);

        public static implicit operator DbNumber(double value) => Create(value);

        public static implicit operator DbNumber(decimal value) => Create(value);

        // C# / Visual Studio bug. if Obsolete as error, compiler will choose other version,but code editor tooltip display wrong overloaded method.
        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(short? value) => Create(value);

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(int? value) => Create(value);

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(long? value) => Create(value);

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(float? value) => Create(value);

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(double? value) => Create(value);

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator DbNumber(decimal? value) => Create(value);
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(DbNumber a, DbNumber b) => a.IsEquals(b);

        public static SqlBoolean operator !=(DbNumber a, DbNumber b) => a.NotEquals(b);

        public static SqlBoolean operator >(DbNumber a, DbNumber b) => a.GreatThan(b);

        public static SqlBoolean operator >=(DbNumber a, DbNumber b) => a.GreatThanOrEquals(b);

        public static SqlBoolean operator <(DbNumber a, DbNumber b) => a.LessThan(b);

        public static SqlBoolean operator <=(DbNumber a, DbNumber b) => a.LessThanOrEquals(b);
        #endregion
        #region Custom Operator
        public static DbNumber operator +(DbNumber a) => Operatior.Unary("+", a).Create<DbNumber>();

        public static DbNumber operator -(DbNumber a) => Operatior.Unary("-", a).Create<DbNumber>();

        public static DbNumber operator +(DbNumber a, DbNumber b) => Operatior.Binary(a, "+", b).Create<DbNumber>();

        public static DbNumber operator -(DbNumber a, DbNumber b) => Operatior.Binary(a, "-", b).Create<DbNumber>();

        public static DbNumber operator *(DbNumber a, DbNumber b) => Operatior.Binary(a, "-", b).Create<DbNumber>();

        public static DbNumber operator /(DbNumber a, DbNumber b) => Operatior.Binary(a, "-", b).Create<DbNumber>();

        public static DbNumber operator %(DbNumber a, DbNumber b) => Function.Call("MOD", a, b).Create<DbNumber>();
        #endregion
        #region Methods
        public SqlBoolean Equals(DbNumber other) => this == other;

        [Obsolete("Invalid SQL expression")]
        public new SqlBoolean Equals(object obj) => obj is DbNumber ? this == (DbNumber)obj : new SqlBoolean();

        internal NullableDbNumber ToNullable() => this._sqlBuilder.Create<NullableDbNumber>();
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression => _sqlBuilder == null;
        #endregion
    }
    #endregion

    #region Char?
    public struct NullableDbChar : IDbExpression<char?>
    {
        public DbChar GetValueOrDefault(DbChar defaultValue) => Function.Call("NVL", this, defaultValue).Create<DbChar>();

        public static implicit operator NullableDbChar(char? value)
        {
            if (!value.HasValue)
                return new NullableDbChar();

            return SqlParameter.Create(value.Value).Create<NullableDbChar>();
        }

        #region Comparision Operators
        public static SqlBoolean operator ==(NullableDbChar a, NullableDbChar b) => a.IsEquals(b);

        public static SqlBoolean operator !=(NullableDbChar a, NullableDbChar b) => a.NotEquals(b);

        public static SqlBoolean operator >(NullableDbChar a, NullableDbChar b) => a.GreatThan(b);

        public static SqlBoolean operator >=(NullableDbChar a, NullableDbChar b) => a.GreatThanOrEquals(b);

        public static SqlBoolean operator <(NullableDbChar a, NullableDbChar b) => a.LessThan(b);

        public static SqlBoolean operator <=(NullableDbChar a, NullableDbChar b) => a.LessThanOrEquals(b);
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression => _sqlBuilder == null;
        #endregion
    }
    #endregion

    #region Enum?
    public struct NullableEnum<T> : IDbExpression<T?> where T : struct
    {
        public Enum<T> GetValueOrDefault(Enum<T> defaultValue) => Function.Call("NVL", this, defaultValue).Create<Enum<T>>();

        public static implicit operator NullableEnum<T>(T? value)
        {
            if (!value.HasValue)
                return new NullableEnum<T>();

            return SqlParameter.Create(Enum.GetName(typeof(T), value.Value)).Create<NullableEnum<T>>();
        }
        #region Comparision Operators
        public static SqlBoolean operator ==(NullableEnum<T> a, NullableEnum<T> b) => a.IsEquals(b);

        public static SqlBoolean operator !=(NullableEnum<T> a, NullableEnum<T> b) => a.NotEquals(b);

        public static SqlBoolean operator >(NullableEnum<T> a, NullableEnum<T> b) => a.GreatThan(b);

        public static SqlBoolean operator >=(NullableEnum<T> a, NullableEnum<T> b) => a.GreatThanOrEquals(b);

        public static SqlBoolean operator <(NullableEnum<T> a, NullableEnum<T> b) => a.LessThan(b);

        public static SqlBoolean operator <=(NullableEnum<T> a, NullableEnum<T> b) => a.LessThanOrEquals(b);
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression => _sqlBuilder == null;
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

        public DbDateTime GetValueOrDefault(DbDateTime defaultValue) => Function.Call("NVL", this, defaultValue).Create<DbDateTime>();

        public static implicit operator System.DateTime? (NullableDbDateTime @this) => @this._valueProvider();

        public static implicit operator NullableDbDateTime(DateTime? value) => !value.HasValue ? new NullableDbDateTime() : SqlParameter.Create(value.Value).Create<NullableDbDateTime>();

        public static implicit operator NullableDbDateTime(DbDateTime value) => value.ToNullable();

        public static NullableDbTimeSpan operator -(NullableDbDateTime a, NullableDbDateTime b) => Operatior.Binary(a, " - ", b).Create<NullableDbTimeSpan>();

        #region Comparision Operators
        public static SqlBoolean operator ==(NullableDbDateTime a, NullableDbDateTime b) => a.IsEquals(b);

        public static SqlBoolean operator !=(NullableDbDateTime a, NullableDbDateTime b) => a.NotEquals(b);

        public static SqlBoolean operator >(NullableDbDateTime a, NullableDbDateTime b) => a.GreatThan(b);

        public static SqlBoolean operator >=(NullableDbDateTime a, NullableDbDateTime b) => a.GreatThanOrEquals(b);

        public static SqlBoolean operator <(NullableDbDateTime a, NullableDbDateTime b) => a.LessThan(b);

        public static SqlBoolean operator <=(NullableDbDateTime a, NullableDbDateTime b) => a.LessThanOrEquals(b);
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression => _sqlBuilder == null;
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

        public static implicit operator System.TimeSpan? (NullableDbTimeSpan @this) => @this._valueProvider();

        public static implicit operator NullableDbTimeSpan(TimeSpan? value) => !value.HasValue ? new NullableDbTimeSpan() : SqlParameter.Create(value.Value).Create<NullableDbTimeSpan>();

        public static implicit operator NullableDbTimeSpan(DbTimeSpan value) => value.ToNullable();

        public DbTimeSpan GetValueOrDefault(DbTimeSpan defaultValue) => Function.Call("NVL", this, defaultValue).Create<DbTimeSpan>();

        #region Comparision Operators
        public static SqlBoolean operator ==(NullableDbTimeSpan a, NullableDbTimeSpan b) => a.IsEquals(b);

        public static SqlBoolean operator !=(NullableDbTimeSpan a, NullableDbTimeSpan b) => a.NotEquals(b);

        public static SqlBoolean operator >(NullableDbTimeSpan a, NullableDbTimeSpan b) => a.GreatThan(b);

        public static SqlBoolean operator >=(NullableDbTimeSpan a, NullableDbTimeSpan b) => a.GreatThanOrEquals(b);

        public static SqlBoolean operator <(NullableDbTimeSpan a, NullableDbTimeSpan b) => a.LessThan(b);

        public static SqlBoolean operator <=(NullableDbTimeSpan a, NullableDbTimeSpan b) => a.LessThanOrEquals(b);
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression => _sqlBuilder == null;
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

        public DbNumber GetValueOrDefault(DbNumber defaultValue) => Function.Call("NVL", this, defaultValue).Create<DbNumber>();

        #region Conversion Operator
        public static implicit operator short? (NullableDbNumber @this) => (short?)@this._valueProvider();

        public static implicit operator int? (NullableDbNumber @this) => (int?)@this._valueProvider();

        public static implicit operator long? (NullableDbNumber @this) => (long?)@this._valueProvider();

        public static implicit operator float? (NullableDbNumber @this) => (float?)@this._valueProvider();

        public static implicit operator double? (NullableDbNumber @this) => (double?)@this._valueProvider();

        public static implicit operator decimal? (NullableDbNumber @this) => @this._valueProvider();

        public static implicit operator NullableDbNumber(DbNumber value) => value.ToNullable();

        static NullableDbNumber Create<T>(T? value) where T : struct => !value.HasValue ? new NullableDbNumber() : SqlParameter.Create(value.Value).Create<NullableDbNumber>();

        public static implicit operator NullableDbNumber(short? value) => Create(value);

        public static implicit operator NullableDbNumber(int? value) => Create(value);

        public static implicit operator NullableDbNumber(long? value) => Create(value);

        public static implicit operator NullableDbNumber(float? value) => Create(value);

        public static implicit operator NullableDbNumber(double? value) => Create(value);

        public static implicit operator NullableDbNumber(decimal? value) => Create(value);
        #endregion
        #region Comparision Operator
        public static SqlBoolean operator ==(NullableDbNumber a, NullableDbNumber b) => a.IsEquals(b);

        public static SqlBoolean operator !=(NullableDbNumber a, NullableDbNumber b) => a.NotEquals(b);

        public static SqlBoolean operator >(NullableDbNumber a, NullableDbNumber b) => a.GreatThan(b);

        public static SqlBoolean operator >=(NullableDbNumber a, NullableDbNumber b) => a.GreatThanOrEquals(b);

        public static SqlBoolean operator <(NullableDbNumber a, NullableDbNumber b) => a.LessThan(b);

        public static SqlBoolean operator <=(NullableDbNumber a, NullableDbNumber b) => a.LessThanOrEquals(b);
        #endregion
        #region Custom Operator
        public static NullableDbNumber operator +(NullableDbNumber a) => Operatior.Unary("+", a).Create<NullableDbNumber>();

        public static NullableDbNumber operator -(NullableDbNumber a) => Operatior.Unary("-", a).Create<NullableDbNumber>();

        public static NullableDbNumber operator +(NullableDbNumber a, NullableDbNumber b) => Operatior.Binary(a, "+", b).Create<NullableDbNumber>();

        public static NullableDbNumber operator -(NullableDbNumber a, NullableDbNumber b) => Operatior.Binary(a, "-", b).Create<NullableDbNumber>();

        public static NullableDbNumber operator *(NullableDbNumber a, NullableDbNumber b) => Operatior.Binary(a, "-", b).Create<NullableDbNumber>();

        public static NullableDbNumber operator /(NullableDbNumber a, NullableDbNumber b) => Operatior.Binary(a, "-", b).Create<NullableDbNumber>();
        #endregion
        #region IDbExpression
        SqlGenerator _sqlBuilder;
        SqlGenerator IDbExpression.Build { get { return _sqlBuilder; } set { _sqlBuilder = value; } }
        bool IDbExpression.IsNullExpression => _sqlBuilder == null;
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
        bool IDbExpression.IsNullExpression => false;
        #endregion
    }
}
