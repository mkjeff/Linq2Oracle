using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Xml.Serialization;

namespace Linq2Oracle
{
    [Serializable]
    public abstract class DbEntity : INotifyPropertyChanged
    {
        [XmlIgnore]
        public bool IsLoaded { get; internal set; }

        public bool IsChanged => ChangedMap.Count > 0;

        /// <summary>
        /// key: ColumnIndex, value: originalValue(db value)
        /// </summary>
        [NonSerialized]
        SortedList<int, object> _changedMap = _EmptyChangeMap;//有欄位變更才初始化

        [OnDeserialized]
        void Init(StreamingContext context)
        {
            _changedMap = _EmptyChangeMap;
        }

        static readonly SortedList<int, object> _EmptyChangeMap = new SortedList<int, object>(0);

        internal SortedList<int, object> ChangedMap => _changedMap;

        protected void BeforeColumnChange([CallerMemberNameAttribute]string columnName = "")
        {
            if (!IsLoaded)
                return;

            var tableInfo = Table.GetTableInfo(this.GetType());

            DbColumn c;
            if (!tableInfo.DbColumnMap.TryGetValue(columnName, out c))
                return;

            if (_changedMap == _EmptyChangeMap)
                _changedMap = new SortedList<int, object>();

            if (!_changedMap.ContainsKey(c.ColumnIndex))
            {
                _changedMap.Add(c.ColumnIndex, c.GetValue(this));
                NotifyPropertyChanged(nameof(IsChanged));
            }
        }

        internal protected virtual void OnSaving() { }

        #region INotifyPropertyChanged 成員

        public event PropertyChangedEventHandler PropertyChanged;

        protected void NotifyPropertyChanged([CallerMemberNameAttribute]string propertyName = "")
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
        #endregion
    }
}
