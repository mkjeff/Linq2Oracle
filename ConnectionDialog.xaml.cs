using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using System.IO;
using LINQPad.Extensibility.DataContext;
using System.Data;

namespace Linq2Oracle.LinqPad
{
	/// <summary>
	/// Interaction logic for ConnectionDialog.xaml
	/// </summary>
	public partial class ConnectionDialog : Window
	{
		IConnectionInfo _cxInfo;

		public ConnectionDialog(IConnectionInfo cxInfo)
		{
			_cxInfo = cxInfo;
			DataContext = cxInfo.CustomTypeInfo;
			InitializeComponent();
		}

		void btnOK_Click (object sender, RoutedEventArgs e)
		{
			DialogResult = true;
		}

		void BrowseAssembly (object sender, RoutedEventArgs e)
		{
			var dialog = new Microsoft.Win32.OpenFileDialog ()
			{
				Title = "Choose custom assembly",
				DefaultExt = ".dll",
			};

			if (dialog.ShowDialog () == true)
				_cxInfo.CustomTypeInfo.CustomAssemblyPath = dialog.FileName;
		}

		void BrowseAppConfig (object sender, RoutedEventArgs e)
		{
			var dialog = new Microsoft.Win32.OpenFileDialog ()
			{
				Title = "Choose application config file",
				DefaultExt = ".config",
			};

			if (dialog.ShowDialog () == true)
				_cxInfo.AppConfigPath = dialog.FileName;
		}

		void ChooseType (object sender, RoutedEventArgs e)
		{
			string assemPath = _cxInfo.CustomTypeInfo.CustomAssemblyPath;
			if (assemPath.Length == 0)
			{
				MessageBox.Show ("First enter a path to an assembly.");
				return;
			}

			if (!File.Exists (assemPath))
			{
				MessageBox.Show ("File '" + assemPath + "' does not exist.");
				return;
			}

			string[] customTypes;
			try
			{
				// TODO: In a real-world driver, call the method accepting a base type instead (unless you're.
				// working with a POCO ORM). For instance: GetCustomTypesInAssembly ("System.Data.Linq.DataContext")
				// You can put interfaces in here, too.
				customTypes = _cxInfo.CustomTypeInfo.GetCustomTypesInAssembly("Linq2Oracle.OracleDB");
			}
			catch (Exception ex)
			{
				MessageBox.Show ("Error obtaining custom types: " + ex.Message);
				return;
			}
			if (customTypes.Length == 0)
			{
				MessageBox.Show ("There are no public types in that assembly.");  // based on.........
				return;
			}

			string result = (string) LINQPad.Extensibility.DataContext.UI.Dialogs.PickFromList ("Choose Custom Type", customTypes);
			if (result != null) _cxInfo.CustomTypeInfo.CustomTypeName = result;
		}

		private void btnTest_Click(object sender, RoutedEventArgs e)
		{
			try
			{
				using (IDbConnection connection = this._cxInfo.DatabaseInfo.GetConnection())
				{
					connection.Open();
					MessageBox.Show("Connection Successful");
				}
			}
			catch (Exception ex)
			{
				MessageBox.Show(ex.Message, "Connection Fail");
			}
		}
	}

	[ValueConversion(typeof(bool), typeof(bool))]
	public class InverseBooleanConverter : IValueConverter
	{
		#region IValueConverter Members

		public object Convert(object value, Type targetType, object parameter,
			System.Globalization.CultureInfo culture)
		{
			if (targetType != typeof(bool))
				throw new InvalidOperationException("The target must be a boolean");

			return !(bool)value;
		}

		public object ConvertBack(object value, Type targetType, object parameter,
			System.Globalization.CultureInfo culture)
		{
			throw new NotSupportedException();
		}

		#endregion
	}
}
