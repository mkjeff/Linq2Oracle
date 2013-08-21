using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Linq2Oracle
{
    sealed class LocationalIndex:IEquatable<LocationalIndex>
    {
        readonly string _file;
        readonly int _line;
        readonly int _hashCode;
        public LocationalIndex(string filename,int lineNumber){
            _file = filename;
            _line = lineNumber;
            _hashCode = _file.GetHashCode() ^ _line;
        }

        public override bool Equals(object obj)
        {
            return this.Equals(obj as LocationalIndex);
        }

        public override int GetHashCode()
        {
            return _hashCode;
        }

        public bool Equals(LocationalIndex other)
        {
            if (other == null)
                return false;
            return object.ReferenceEquals(_file, other._file) && _line == other._line;
        }
    }
}
