using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.ComponentModel;
using System.Reflection;

using ExampleSatelite.Sage50.Datos;

// Clases de Sage50
using sage.ew.db;
using sage.ew.functions;

namespace ExampleSatelite.Sage50.Negocio
{
    public class BaseMaster
    {
        public clsDatos loDatos = new clsDatos();

        public string psDb = "";
        public string psTable = "";
        public string _Error_Message = "";


        public string Error()
        {
            return this._Error_Message.Trim();
        }

        /// <summary>
        /// Leer los registro de la tabla
        /// Regresa un DataTable con el resultado, con el parametro tsSelect, se puede escoger solo los campos que regresaria en el datatable
        /// Si no se indica valor en el parametro regresará todos los campos de la tabla
        /// </summary>
        /// <param name="tsCodigo"></param>
        /// <param name="tsSelect"></param>
        /// <returns></returns>
        public virtual DataTable _LoadTable(string tsCodigo, string tsSelect = "")
        {
            DataTable loResul = new DataTable();
            string lsSQL = "";

            if (string.IsNullOrEmpty(tsSelect))
                tsSelect = "*";

            lsSQL = "Select " + tsSelect.Trim() + " " +
                "From " + loDatos._SQLDataBase(psDb, psTable) + " ";

            if (!string.IsNullOrEmpty(tsCodigo))
                lsSQL += "Where codigo='" + tsCodigo.Trim() + "' ";
            else
                lsSQL += "Order By codigo ";

            if (!loDatos._SQLexec(lsSQL, ref loResul))
                this._Error_Message = loDatos._Error_Message.Trim();

            return loResul;
        }

        /// <summary>
        /// Leer un registro de la tabla
        /// Regresa un DataTable con el resultado, con el parametro tsSelect, se puede escoger solo los campos que regresaria en el datatable
        /// usando el "a." como alias de cada campo. Si no se indica valor en el parametro regresará todos los campos de la tabla
        /// </summary>
        /// <param name="tsSelect"></param>
        /// <param name="tsWhere"></param>
        /// <returns></returns>
        public virtual DataRow _RowValue(string tsSelect, string tsWhere)
        {
            DataTable ldtData = new DataTable();
            string lsSQL = "Select {0} From {1} Where {2} ";
            Boolean lbOk = false;

            lsSQL = string.Format(lsSQL, tsSelect.Trim(), loDatos._SQLDataBase(psDb, psTable), tsWhere.Trim());

            lbOk = loDatos._SQLexec(lsSQL, ref ldtData);
            DataRow ldrRow = ldtData.NewRow();
            
            if (ldtData.Rows.Count > 0)
                ldrRow = ldtData.Rows[0];
            else
            {
                ldtData.Rows.InsertAt(ldrRow, 0);
                ldrRow = ldtData.Rows[0];
            }
            ldtData.Dispose();

            return ldrRow;
        }


        public virtual Boolean _Load(ref dynamic toEntity)
        {
            return true;
        }

        public virtual Boolean _Create(dynamic toEntity)
        {
            return true;
        }

        public virtual Boolean _Update(dynamic toEntity)
        {
            return true;
        }

        public virtual Boolean _Delete(dynamic toEntity)
        {
            return true;
        }

    }
}
