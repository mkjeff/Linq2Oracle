using Linq2Oracle;
using LINQPad;
using LINQPad.Extensibility.DataContext;
using Microsoft.CSharp;
using Oracle.ManagedDataAccess.Client;
using System;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Windows.Controls;

namespace Linq2Oracle.LinqPad
{
    /// <summary>
    /// Sample dynamic driver. This lets users connect to an ADO.NET Data Services URI, builds the
    /// type data context dynamically, and returns objects for the Schema Explorer.
    /// </summary>
    public class DataAccessLibDynamicDriver : DynamicDataContextDriver
    {
        public override string Name { get { return "Linq2Oracle Dynamic Driver"; } }

        public override string Author { get { return "mkjeff"; } }

        [Obsolete]
        public override IEnumerable<string> GetAssembliesToAdd()
        {
            return new string[] { 
				"Oracle.ManagedDataAccess.dll",
				"Linq2Oracle.dll",
			};
        }

        [Obsolete]
        public override IEnumerable<string> GetNamespacesToAdd()
        {
            return new string[]{
				"Linq2Oracle",
			};
        }

        public override ICustomMemberProvider GetCustomDisplayMemberProvider(object objectToWrite)
        {
            var entity = objectToWrite as DbEntity;
            if (entity != null)
                return CustomMemberProvider.GetProvider(entity);
            return base.GetCustomDisplayMemberProvider(objectToWrite);
        }

        public override System.Data.Common.DbProviderFactory GetProviderFactory(IConnectionInfo cxInfo)
        {
            return new OracleClientFactory();
        }

        public override ParameterDescriptor[] GetContextConstructorParameters(IConnectionInfo cxInfo)
        {
            return new[] { new ParameterDescriptor("connection", "Oracle.ManagedDataAccess.Client.OracleConnection") };
        }

        public override object[] GetContextConstructorArguments(IConnectionInfo cxInfo)
        {
            return new object[] { cxInfo.DatabaseInfo.GetConnection() };
        }

        public override void ClearConnectionPools(IConnectionInfo cxInfo)
        {
            using (var oracleConnection = (OracleConnection)cxInfo.DatabaseInfo.GetConnection())
                OracleConnection.ClearPool(oracleConnection);
        }

        public override bool AreRepositoriesEquivalent(IConnectionInfo c1, IConnectionInfo c2)
        {
            var conn1 = new OracleConnectionStringBuilder(c1.DatabaseInfo.CustomCxString);
            var conn2 = new OracleConnectionStringBuilder(c2.DatabaseInfo.CustomCxString);

            return conn1.UserID == conn2.UserID && conn1.DataSource == conn2.DataSource;
        }

        public override string GetConnectionDescription(IConnectionInfo cxInfo)
        {
            var conn = new OracleConnectionStringBuilder(cxInfo.DatabaseInfo.CustomCxString);
            // For static drivers, we can use the description of the custom type & its assembly:
            return conn.UserID.ToUpper() + "." + conn.DataSource.Substring(conn.DataSource.LastIndexOf('/') + 1);
        }

        public override bool ShowConnectionDialog(IConnectionInfo cxInfo, bool isNewConnection)
        {
            // Prompt the user for a custom assembly and type name:
            return new ConnectionDialog(cxInfo).ShowDialog() == true;
        }

        public override void InitializeContext(IConnectionInfo cxInfo, object context, QueryExecutionManager executionManager)
        {
            // If the data context happens to be a LINQ to SQL DataContext, we can look up the SQL translation window.
            var l2s = context as OracleDB;
            if (l2s != null) l2s.Log = executionManager.SqlTranslationWriter;
        }

        public override void DisplayObjectInGrid(object objectToDisplay, GridOptions options)
        {
            var iqtx = objectToDisplay.GetType().GetInterface("IQueryContext`1");
            if (iqtx != null && typeof(DbEntity).IsAssignableFrom(iqtx.GetGenericArguments()[0]) || objectToDisplay is DbEntity)
            {
                options.MembersToExclude = new string[] { "IsLoaded", "IsChanged" };
                //new DataGrid { ItemsSource = (IEnumerable)objectToDisplay }.Dump(options.PanelTitle);
                //return;
            }
            //else if (objectToDisplay is DbEntity)
            //{
            //    //new PropertyGrid { DataSource = objectToDisplay }.Dump(options.PanelTitle);
            //    options.MembersToExclude = new string[] { "IsLoaded", "IsChanged" };
            //}
            base.DisplayObjectInGrid(objectToDisplay, options);
        }

        public override void PreprocessObjectToWrite(ref object objectToWrite, ObjectGraphInfo info)
        {
            if (objectToWrite == null)
                return;
            var type = objectToWrite.GetType();
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(GroupingContext<,,,>))
                objectToWrite = Activator.CreateInstance(typeof(GroupContextWrapper<,,,>).MakeGenericType(type.GetGenericArguments()), objectToWrite);
        }

        public override bool DisallowQueryDisassembly { get { return true; } }

        //public override DateTime? GetLastSchemaUpdate(IConnectionInfo cxInfo)
        //{
        //    using (var oracleConnection = (OracleConnection)cxInfo.DatabaseInfo.GetConnection())
        //    {
        //        oracleConnection.Open();
        //        OracleCommand oracleCommand = new OracleCommand("SELECT MAX (LAST_DDL_TIME) FROM SYS.ALL_OBJECTS", oracleConnection);
        //        try
        //        {
        //            DateTime value = (DateTime)oracleCommand.ExecuteScalar();
        //            return value;
        //        }
        //        catch
        //        {
        //            return null;
        //        }
        //    }
        //}

        public override List<ExplorerItem> GetSchemaAndBuildAssembly(IConnectionInfo cxInfo, AssemblyName assemblyToBuild,
            ref string nameSpace, ref string typeName)
        {
            using (var con = (OracleConnection)cxInfo.DatabaseInfo.GetConnection())
            {
                // Customize.If you need enum type mapping.
                var enumTypeMap = new Dictionary<string, Type>();
                con.Open();

                var schema = new OracleConnectionStringBuilder(con.ConnectionString).UserID.ToUpper();
                var res = new[] { schema };
                var alltables = con.GetSchema("Tables", res).AsEnumerable().ToArray();
                var allcolumns = con.GetSchema("Columns", res);
                var allpks = con.GetSchema("PrimaryKeys", res).AsEnumerable().ToArray();
                var allindexs = con.GetSchema("IndexColumns", res).AsEnumerable().ToArray();
                var allviews = con.GetSchema("Views", res).AsEnumerable().ToArray();
                var tables = (from column in allcolumns.AsEnumerable()
                              where alltables.Any(r => r.Field<string>("table_name") == column.Field<string>("table_name"))
                                || allviews.Any(r => r.Field<string>("view_name") == column.Field<string>("table_name"))
                              group column by new
                              {
                                  TableName = column.Field<string>("table_name"),
                                  IsView = allviews.Any(r => r.Field<string>("view_name") == column.Field<string>("table_name"))
                              } into tableColumns
                              select new
                              {
                                  IsView = tableColumns.Key.IsView,
                                  TableName = tableColumns.Key.TableName,
                                  Columns = (from c in tableColumns
                                             let columnName = c.Field<string>("column_name")
                                             let dataType = c.Field<string>("datatype")
                                             let isNullable = c.Field<string>("nullable") == "Y"
                                             let length = c.Field<decimal>("length")
                                             let precision = c.Field<decimal?>("precision")
                                             let scale = c.Field<decimal?>("scale")
                                             select new
                                             {
                                                 IsPrimaryKey = (from idx in allindexs
                                                                 where idx.Field<string>("table_name") == tableColumns.Key.TableName
                                                                    && idx.Field<string>("column_name") == columnName
                                                                 from pk in allpks
                                                                 where pk.Field<string>("index_name") == idx.Field<string>("index_name")
                                                                 select pk).Any(),
                                                 ColumnName = columnName,
                                                 FieldName = "_" + columnName,
                                                 DataType = dataType,
                                                 Length = length,
                                                 Precision = precision,
                                                 Scale = scale,
                                                 IsNullable = isNullable,
                                                 ClrType = OracleType2Clr(enumTypeMap, tableColumns.Key.TableName, columnName, isNullable, dataType, length, precision, scale),
                                                 DbType = OracleType2OracleDbType(dataType, length, precision, scale)
                                             }).ToList(),
                              }).ToList();
                var source = new StringBuilder(@"using System;
using System.Diagnostics;
using System.Text;
using Linq2Oracle;
using Oracle.ManagedDataAccess.Client;");
                source.Append("namespace ").Append(nameSpace).AppendLine("{");
                foreach (var table in tables)
                {
                    source.Append("public sealed partial class ").Append(table.TableName).Append(" : DbEntity , IEquatable<").Append(table.TableName).AppendLine(">{");
                    if (table.Columns.Any(c => c.IsPrimaryKey))
                    {
                        var pkColumns = (from pk in table.Columns where pk.IsPrimaryKey select pk).ToArray();
                        source.Append("public bool Equals(").Append(table.TableName).AppendLine("  other) {")
                        .AppendLine("if (other == null) return false;")
                        .Append("return ").Append(string.Join(" && ", from pk in pkColumns select string.Format("{0} == other.{0}", pk.FieldName))).AppendLine(";")
                        .AppendLine("}")
                        .AppendLine("public override bool Equals(object obj) {")
                        .Append("return Equals(obj as ").Append(table.TableName).AppendLine(");")
                        .AppendLine("}")
                        .AppendLine("public override int GetHashCode() {")
                        .Append("return ").Append(string.Join(" ^ ", from pk in pkColumns select string.Format("{0}.GetHashCode()", pk.FieldName))).AppendLine(";")
                        .AppendLine("}");
                    }
                    else
                    {
                        source.Append("bool IEquatable<").Append(table.TableName).Append(">.Equals(").Append(table.TableName).AppendLine(" other) {")
                            .AppendLine("return this.Equals(other);")
                            .AppendLine("}");
                    }
                    foreach (var c in table.Columns)
                    {
                        string strClrType = GetFriendlyName(c.ClrType);
                        source.Append(strClrType).Append(" _").Append(c.ColumnName).AppendLine(";")
                            .Append("[Column(Size=").Append(c.Length).Append(", DbType=OracleDbType.").Append(c.DbType).Append(", IsNullable = ").Append(c.IsNullable ? "true" : "false").Append(", IsPrimarykey = ").Append(c.IsPrimaryKey ? "true" : "false").AppendLine(")]")
                            .Append("public ").Append(GetFriendlyName(c.ClrType)).Append(" ").Append(c.ColumnName).AppendLine("{")
                            .Append("get{ return _").Append(c.ColumnName).AppendLine(";}")
                            .AppendLine("set{ ")
                            .Append("if(_").Append(c.ColumnName).AppendLine("!= value){")
                            .AppendLine("BeforeColumnChange();")
                            .Append("_").Append(c.ColumnName).AppendLine(" = value;")
                            .AppendLine("NotifyPropertyChanged();")
                            .AppendLine("}")
                            .AppendLine("}")
                            .AppendLine("}");
                    }
                    source.AppendLine("public sealed class Columns {");
                    foreach (var c in table.Columns)
                    {
                        string strClrType = ToQueryTypeString(c.ClrType);
                        source.Append("public ").Append(strClrType).Append(" ").Append(c.ColumnName).AppendLine(" { get; private set; }");
                    }
                    source.AppendLine("}");
                    source.AppendLine("}");
                }

                typeName = schema + "Database";

                source.Append("public partial class ").Append(typeName).AppendLine(": OracleDB {");
                source.Append("public ").Append(typeName).AppendLine("(OracleConnection connection):base(connection) { }");

                foreach (var table in tables)
                {
                    source.Append("public EntityTable<").Append(table.TableName).Append(",").Append(table.TableName).Append(".Columns> ").Append(table.TableName).Append(" { get { return new EntityTable<").Append(table.TableName).Append(",").Append(table.TableName).AppendLine(".Columns>(this); } }");
                }

                source.AppendLine("}");
                source.Append("}");


                BuildAssembly(source.ToString(), assemblyToBuild);

                var topLevelProps =
                    from prop in tables
                    orderby prop.IsView descending, prop.TableName
                    select new ExplorerItem(prop.TableName, ExplorerItemKind.QueryableObject, prop.IsView ? ExplorerIcon.View : ExplorerIcon.Table)
                    {
                        IsEnumerable = true,
                        ToolTipText = prop.TableName,
                        Children = (from column in prop.Columns
                                    orderby column.IsPrimaryKey descending, column.ColumnName
                                    select new ExplorerItem(column.ColumnName + " (" + FormatTypeName(column.ClrType, false) + ")",
                                            ExplorerItemKind.Property,
                                            column.IsPrimaryKey ? ExplorerIcon.Key : ExplorerIcon.Column)
                                            {
                                                ToolTipText = column.ColumnName + " " + column.DbType.ToString().ToUpper() + "(" + column.Length + ")" + (column.IsNullable ? string.Empty : " NOT NULL")
                                            }).ToList()
                    };

                return topLevelProps.ToList();
            }
        }



        static void BuildAssembly(string code, AssemblyName name)
        {
            // Use the CSharpCodeProvider to compile the generated code:
            CompilerResults results;
            using (var codeProvider = new CSharpCodeProvider(new Dictionary<string, string>() { { "CompilerVersion", "v4.0" } }))
            {
                var options = new CompilerParameters(
                    "System.dll System.Data.dll System.Core.dll System.Xml.dll Linq2Oracle.dll Oracle.ManagedDataAccess.dll".Split(),
                    name.CodeBase,
                    false) { CompilerOptions = "/lib:\"" + Path.GetDirectoryName(typeof(DbEntity).Assembly.Location) + "\"" };
                results = codeProvider.CompileAssemblyFromSource(options, code);
            }
            if (results.Errors.Cast<CompilerError>().Where(e => !e.IsWarning).Any())
            {
                var firstError = results.Errors.Cast<CompilerError>().First(e => !e.IsWarning);
                throw new Exception("Cannot compile typed context: " + firstError.ErrorText + " (line " + firstError.Line + ")");

            }
        }

        bool IsEnumType(Dictionary<string, Type> enumTypeMap, string tableName, string columnName)
        {
            return enumTypeMap.ContainsKey(tableName + "." + columnName);
        }

        string ToQueryTypeString(Type t)
        {
            Type nonNullable = GetNonNullType(t);
            if (nonNullable == typeof(string))
                return "Linq2Oracle.String";
            if (nonNullable.IsEnum)
                return "Linq2Oracle.Enum<" + GetFriendlyName(t) + ">";
            if (nonNullable == typeof(DateTime))
                return "Linq2Oracle.DateTime<" + GetFriendlyName(t) + ">";
            if (nonNullable == typeof(char))
                return "Linq2Oracle.DbExpression<" + GetFriendlyName(t) + ">";
            return "Linq2Oracle.Number<" + GetFriendlyName(t) + ">";
        }

        static Type GetNonNullType(Type t)
        {
            if (t.IsGenericType && t.GetGenericTypeDefinition() == (typeof(Nullable<>)))
                return t.GetGenericArguments()[0];
            return t;
        }

        static string GetFriendlyName(Type t)
        {
            using (var provider = new CSharpCodeProvider())
            {
                var typeRef = new CodeTypeReference(t);
                return provider.GetTypeOutput(typeRef);
            }
        }

        Type OracleType2Clr(Dictionary<string, Type> enumTypeMap, string tableName, string columnName, bool isNullable, string oracleType, decimal length, decimal? precision, decimal? scale)
        {
            if (oracleType.IndexOf("CHAR") != -1)
            {
                if (length == 1)
                    return isNullable ? typeof(char?) : typeof(char);
                Type enumType;
                if (enumTypeMap.TryGetValue(tableName + "." + columnName, out enumType))
                    if (isNullable)
                        return typeof(Nullable<>).MakeGenericType(enumType);
                    else
                        return enumType;
                return typeof(string);
            }

            if (oracleType == "NUMBER")
            {
                if (precision == null)
                {
                    if (length > 22)
                        return isNullable ? typeof(decimal?) : typeof(decimal);
                    if (length > 18)
                        return isNullable ? typeof(long?) : typeof(long);
                    if (length > 9)
                        return isNullable ? typeof(int?) : typeof(int);
                    if (length > 4)
                        return isNullable ? typeof(short?) : typeof(short);
                    return isNullable ? typeof(byte?) : typeof(byte);
                }
                else
                {
                    if (precision >= 19)
                        return isNullable ? typeof(decimal?) : typeof(decimal);
                    if (precision >= 10)
                        return isNullable ? typeof(long?) : typeof(long);
                    if (precision >= 6)
                        return isNullable ? typeof(int?) : typeof(int);
                    else
                        return isNullable ? typeof(short?) : typeof(short);
                }
            }

            if (oracleType == "DATE" || oracleType.StartsWith("TIMESTAMP"))
                return isNullable ? typeof(DateTime?) : typeof(DateTime);

            if (oracleType == "FLOAT")
            {
                if (precision > 23)
                    return isNullable ? typeof(double?) : typeof(double);
                return isNullable ? typeof(float?) : typeof(float);
            }

            if (oracleType == "BLOB")
                return typeof(byte[]);

            throw new ApplicationException(tableName + "," + columnName + " Unknown type " + oracleType);
        }

        OracleDbType OracleType2OracleDbType(string oracleType, decimal length, decimal? precision, decimal? scale)
        {
            if (oracleType == "CHAR")
                return OracleDbType.Char;

            if (oracleType == "NCHAR")
                return OracleDbType.NChar;

            if (oracleType == "NVARCHAR2")
                return OracleDbType.NVarchar2;

            if (oracleType == "VARCHAR2")
                return OracleDbType.Varchar2;

            if (oracleType == "NUMBER" && (scale ?? 0) == 0)
            {
                if (precision == null)
                {
                    if (length > 22)
                        return OracleDbType.Decimal;
                    if (length > 18)
                        return OracleDbType.Int64;
                    if (length > 9)
                        return OracleDbType.Int32;
                    if (length > 4)
                        return OracleDbType.Int16;
                    else
                        return OracleDbType.Byte;
                }
                else
                {
                    if (precision >= 19)
                        return OracleDbType.Decimal;
                    if (precision >= 10)
                        return OracleDbType.Int64;
                    if (precision >= 6)
                        return OracleDbType.Int32;
                    else
                        return OracleDbType.Int16;
                }
            }

            if (oracleType == "DATE")
                return OracleDbType.Date;

            if (oracleType.StartsWith("TIMESTAMP"))
                return OracleDbType.TimeStamp;

            if (oracleType == "FLOAT")
            {
                if (precision > 23)
                    return OracleDbType.Double;
                return OracleDbType.Single;
            }

            if (oracleType == "BLOB")
                return OracleDbType.Blob;
            throw new ApplicationException(" Unknown type " + oracleType);
        }
    }
}
