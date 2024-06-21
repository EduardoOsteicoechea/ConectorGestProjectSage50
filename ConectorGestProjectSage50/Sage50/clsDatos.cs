using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

// Clases de Sage50
using sage.ew.db;

namespace ExampleSatelite.Sage50.Datos
{
    public class clsDatos
    {
        #region propiedades        

        public string _Error_Message = "";

        #endregion propiedades

        #region Métodos directo SQL

        /// <summary>
        /// Método para actualizar datos en una tabla para Sage50
        /// NOTA: Debe ser una base de datos reconocida por Sage50
        /// </summary>
        /// <param name="tsDb"></param>
        /// <param name="tsTable"></param>
        /// <param name="tsWhere"></param>
        /// <param name="tsFields"></param>
        /// <returns></returns>
        public Boolean _Update(String tsDb, String tsTable, String tsWhere, String tsFields)
        {
            Boolean lbOk = true;
            String lcTablaOrigen = ReplicatedDB.SQLDatabase(tsDb, tsTable);
            DataTable ldtTabla = new DataTable();

            String lcSQL = "UPDATE " + lcTablaOrigen + " SET " + tsFields + " WHERE " + tsWhere;
            lbOk = this._SQLexec(lcSQL);
            return lbOk;
        }

        /// <summary>
        /// Método para insertar datos en una tabla para Sage50
        /// NOTA: Debe ser una base de datos reconocida por Sage50
        /// </summary>
        /// <param name="tsDb"></param>
        /// <param name="tsTable"></param>
        /// <param name="tsFields"></param>
        /// <param name="tsValues"></param>
        /// <returns></returns>
        public Boolean _Insert(String tsDb, String tsTable, String tsFields, String tsValues)
        {
            Boolean lbOk = false;
            String lcTablaOrigen = ReplicatedDB.SQLDatabase(tsDb, tsTable);
            DataTable ldtTabla = new DataTable();

            String lcSQL = "INSERT INTO " + lcTablaOrigen + "(" + tsFields + ") VALUES(" + tsValues + ")";
            lbOk = this._SQLexec(lcSQL);
            return lbOk;
        }

        /// <summary>
        /// Método para ejecutar comandos SQL estándar en la conexión activa
        /// </summary>
        /// <param name="tsSQL"></param>
        /// <returns></returns>
        public Boolean _SQLexec(String tsSQL)
        {
            Boolean lbOk = false;

            lbOk = ReplicatedDB.SQLExec(tsSQL);

            if (!lbOk)
                this._Error_Message = ReplicatedDB.Error_Message;

            return lbOk;

        }

        /// <summary>
        /// Método para realizar consultas SQL estándar en la conexión activa
        /// regresa por referencia una DataTable con el conjunto de resultados
        /// </summary>
        /// <param name="tsSQL"></param>
        /// <param name="tdtTable"></param>
        /// <returns></returns>
        //
        public Boolean _SQLexec(String tsSQL, ref DataTable tdtTable)
        {
            Boolean lbOk = false;

            lbOk = ReplicatedDB.SQLExec(tsSQL, ref tdtTable);

            if (!lbOk)
                this._Error_Message = ReplicatedDB.Error_Message;

            return lbOk;
        }

        /// <summary>
        /// Crea una base de datos en la conexión activa
        /// Esta base de datos tiene la mismas características que usa Sage50
        /// </summary>
        /// <param name="tsDb"></param>
        /// <returns></returns>
        public Boolean _CreateDB(String tsDb)
        {
            Boolean lbOk = false;

            lbOk = ReplicatedDB._DBCreate(tsDb);

            if (!lbOk)
                this._Error_Message = ReplicatedDB.Error_Message;

            return lbOk;
        }

        /// <summary>
        /// Método para verificar si existe una base de datos en la conexión activa
        /// </summary>
        /// <param name="tsDb"></param>
        /// <returns></returns>
        public Boolean _ExistDB(String tsDb)
        {
            Boolean lbOk = false;

            lbOk = ReplicatedDB._SQLExisteBBDD(tsDb);

            return lbOk;
        }

        /// <summary>
        /// Método para verificar si existe una tabla en una base de datos en la conexión activa
        /// </summary>
        /// <param name="tsDb"></param>
        /// <param name="tsTable"></param>
        /// <returns></returns>
        public Boolean _ExistTable(String tsDb, string tsTable)
        {
            Boolean lbOk = false;

            lbOk = ReplicatedDB._SQLExisteTablaBBDD(tsDb, tsTable);

            return lbOk;
        }


        /// <summary>
        /// Metodo que regresá la cadena SQL del nombre de la base de datos y tabla
        /// ejemplo: _SQLDataBase("COMUNES", "ACTUA") = [comuxxx].dbo.actua
        /// </summary>
        /// <param name="tsDb"></param>
        /// <param name="tsTable"></param>
        /// <returns></returns>
        public string _SQLDataBase(string tsDb, string tsTable)
        {
            return ReplicatedDB.SQLDatabase(tsDb, tsTable);
        }


        #endregion Métodos directo SQL
        
        #region Métodos para procedimientos

        // Consulta directa al eurowinsys
        // No existe una consulta directa desde sage50
        public Boolean Grupos_Multiempresa(string tcCodigo, ref DataTable tdtGrupos)
        {
            Boolean llOk = false;
            string lsSQL = "";

            lsSQL = "Select a.codigo, a.nombre, 'COMU'+a.codigo as Comunes, a.pripal, a.codpripal " +
                    "From " + this._SQLDataBase("eurowinsys", "gruposemp") + " a " +
                    "Where a.codpripal = '" + tcCodigo + "' ";

            ReplicatedDB.SQLExec(lsSQL, ref tdtGrupos);

            llOk = (tdtGrupos.Rows.Count > 0);

            return llOk;
        }

        #endregion Métodos para procedimientos

    }

}
