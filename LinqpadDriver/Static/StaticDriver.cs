using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using LINQPad;
using LINQPad.Extensibility.DataContext;
using Oracle.ManagedDataAccess.Client;

namespace Linq2Oracle.LinqPad
{
    /// <summary>
    /// This static driver let users query any data source that looks like a Data Context - in other words,
    /// that exposes properties of type IEnumerable of T.
    /// </summary>
    public class DataAccessLibStaticDriver : StaticDataContextDriver
    {
        public override string Name => "Linq2Oracle Static Driver";

        public override string Author => "mkjeff";

        [Obsolete]
        public override IEnumerable<string> GetAssembliesToAdd() => new string[] {
                "Oracle.ManagedDataAccess.dll",
                "Linq2Oracle.dll",
            };

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

        public override void PreprocessObjectToWrite(ref object objectToWrite, ObjectGraphInfo info)
        {
            if (objectToWrite == null)
                return;
            var type = objectToWrite.GetType();
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(GroupingContext<,,,>))
                objectToWrite = Activator.CreateInstance(typeof(GroupContextWrapper<,,,>).MakeGenericType(type.GetGenericArguments()), objectToWrite);
        }

        public override bool DisallowQueryDisassembly { get { return true; } }

        public override System.Data.Common.DbProviderFactory GetProviderFactory(IConnectionInfo cxInfo) 
            => new OracleClientFactory();

        public override ParameterDescriptor[] GetContextConstructorParameters(IConnectionInfo cxInfo) 
            => new[] { new ParameterDescriptor("connection", "Oracle.ManagedDataAccess.Client.OracleConnection") };

        public override object[] GetContextConstructorArguments(IConnectionInfo cxInfo) 
            => new object[] { cxInfo.DatabaseInfo.GetConnection() };

        public override void ClearConnectionPools(IConnectionInfo cxInfo)
        {
            using (var oracleConnection = (OracleConnection)cxInfo.DatabaseInfo.GetConnection())
                OracleConnection.ClearPool(oracleConnection);
        }

        // For static drivers, we can use the description of the custom type & its assembly:
        public override string GetConnectionDescription(IConnectionInfo cxInfo) 
            => cxInfo.CustomTypeInfo.GetCustomTypeDescription();

        // Prompt the user for a custom assembly and type name:
        public override bool ShowConnectionDialog(IConnectionInfo cxInfo, bool isNewConnection) 
            => new ConnectionDialog(cxInfo).ShowDialog() == true;

        public override void InitializeContext(IConnectionInfo cxInfo, object context, QueryExecutionManager executionManager)
        {
            // If the data context happens to be a LINQ to SQL DataContext, we can look up the SQL translation window.
            var l2s = context as OracleDB;
            if (l2s != null) l2s.Log = executionManager.SqlTranslationWriter;
        }

        public override List<ExplorerItem> GetSchema(IConnectionInfo cxInfo, Type customType)
        {
            // Return the objects with which to populate the Schema Explorer by reflecting over customType.

            // We'll start by retrieving all the properties of the custom type that implement IEnumerable<T>:
            var topLevelProps =
            (
                from prop in customType.GetProperties()
                where prop.PropertyType != typeof(string)
                // Display all properties of type IEnumerable<T> (except for string!)
                let ienumerableOfT = prop.PropertyType.GetInterface("System.Collections.Generic.IEnumerable`1")
                where ienumerableOfT != null
                orderby prop.Name
                select new ExplorerItem(prop.Name, ExplorerItemKind.QueryableObject, ExplorerIcon.Table)
                {
                    IsEnumerable = true,
                    ToolTipText = FormatTypeName(prop.PropertyType, false),
                    // Store the entity type to the Tag property. We'll use it later.
                    Tag = ienumerableOfT.GetGenericArguments()[0]
                }

            ).ToList();

            // Create a lookup keying each element type to the properties of that type. This will allow
            // us to build hyperlink targets allowing the user to click between associations:
            var elementTypeLookup = topLevelProps.ToLookup(tp => (Type)tp.Tag);

            // Populate the columns (properties) of each entity:
            foreach (ExplorerItem table in topLevelProps)
                table.Children = ((Type)table.Tag)
                    .GetProperties()
                    .Select(childProp => GetChildItem(elementTypeLookup, childProp))
                    .Where(child => child != null)
                    .OrderByDescending(childItem => childItem.Icon)
                    .ToList();

            return topLevelProps;
        }

        ExplorerItem GetChildItem(ILookup<Type, ExplorerItem> elementTypeLookup, PropertyInfo childProp)
        {
            // If the property's type is in our list of entities, then it's a Many:1 (or 1:1) reference.
            // We'll assume it's a Many:1 (we can't reliably identify 1:1s purely from reflection).
            //if (elementTypeLookup.Contains (childProp.PropertyType))
            //    return new ExplorerItem (childProp.Name, ExplorerItemKind.ReferenceLink, ExplorerIcon.ManyToOne)
            //    {
            //        HyperlinkTarget = elementTypeLookup [childProp.PropertyType].First (),
            //        // FormatTypeName is a helper method that returns a nicely formatted type name.
            //        ToolTipText = FormatTypeName (childProp.PropertyType, true)
            //    };

            // Is the property's type a collection of entities?
            //Type ienumerableOfT = childProp.PropertyType.GetInterface ("System.Collections.Generic.IEnumerable`1");
            //if (ienumerableOfT != null)
            //{
            //    Type elementType = ienumerableOfT.GetGenericArguments () [0];
            //    if (elementTypeLookup.Contains (elementType))
            //        return new ExplorerItem (childProp.Name, ExplorerItemKind.CollectionLink, ExplorerIcon.OneToMany)
            //        {
            //            HyperlinkTarget = elementTypeLookup [elementType].First (),
            //            ToolTipText = FormatTypeName (elementType, true)
            //        };
            //}

            var attr = childProp.GetCustomAttribute<ColumnAttribute>(false);
            if (attr == null)
                return null;

            // Ordinary property:
            return new ExplorerItem(
                childProp.Name + " (" + FormatTypeName(childProp.PropertyType, false) + ")",
                ExplorerItemKind.Property,
                attr.IsPrimarykey ? ExplorerIcon.Key : ExplorerIcon.Column)
            {
                ToolTipText = childProp.Name + " " + attr.DbType.ToString().ToUpper() + "(" + attr.Size + ")" + (attr.IsNullable ? string.Empty : " NOT NULL")
            };

        }

        public override void DisplayObjectInGrid(object objectToDisplay, GridOptions options)
        {
            var iqtx = objectToDisplay.GetType().GetInterface("IQueryContext`1");
            if (iqtx != null && typeof(DbEntity).IsAssignableFrom(iqtx.GetGenericArguments()[0]) || objectToDisplay is DbEntity)
            {
                options.MembersToExclude = new string[] { "IsLoaded", "IsChanged" };
            }
            base.DisplayObjectInGrid(objectToDisplay, options);
        }
    }
}
