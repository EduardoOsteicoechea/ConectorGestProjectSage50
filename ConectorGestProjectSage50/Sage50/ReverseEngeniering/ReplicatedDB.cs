#region Assembly sage.ew.db, Version=50.7830.4.0, Culture=neutral, PublicKeyToken=96ae376547cf7c42
// C:\Sage50\Sage50Term\50.7830.4\sage.ew.db.dll
// Decompiled with ICSharpCode.Decompiler 8.1.1.7464
#endregion

using Infragistics.UltraChart.Core.Primitives;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows.Forms;

namespace sage.ew.db
{

    public static class ReplicatedDB
    {
        internal struct Transac
        {
            public SqlConnection _oConexionTransaccion;

            public SqlTransaction _oTransaccion;

            public int _nId;
        }

        public enum Modo_Registro
        {
            Registro_Error,
            Registro_Consulta,
            Indeterminado
        }

        public enum Modo_Analisis
        {
            Indeterminado,
            Manual,
            Automatico,
            Errores,
            Consultas,
            Traza
        }

        public delegate void _SQLEXEC_Before_Delegate(ref string tcSQL, ref bool tlOk);

        public delegate void _SQLEXEC_Before_Parameters_Delegate(ref string tcSQL, ref bool tlOk, ref List<Tuple<string, string, SqlDbType>> parameters);

        public delegate void _SQLEXEC_After_Delegate(ref DataTable tdtResult);

        public class InfoGrupo
        {
            public string Codigo;

            public string Grupo;

            public Dictionary<string, string> Ejercicios;

            public string EjercicioPredet;

            public Dictionary<string, string> Addons;

            public string SQLDataBase(string tcDataBase, string tcEjercicio = "")
            {
                string text = DataBase(tcDataBase, tcEjercicio);
                if(!string.IsNullOrEmpty(text))
                {
                    return $"[{text}].dbo.";
                }

                return string.Empty;
            }

            public string DataBase(string tcDataBase, string tcEjercicio = "")
            {
                string text = tcDataBase;
                string text2;
                if(!(text == "COMUNES"))
                {
                    if(text == "2024XP")
                    {
                        text2 = (Ejercicios.ContainsKey(tcEjercicio) ? Ejercicios[tcEjercicio] : string.Empty);
                    }
                    else
                    {
                        text2 = (Addons.ContainsKey(tcDataBase) ? Addons[tcDataBase] : string.Empty);
                        if(string.IsNullOrEmpty(text2))
                        {
                            List<string> source = Addons.Keys.Where((string f) => f.StartsWith(tcDataBase.Substring(0, 6))).ToList();
                            text2 = Addons[source.First()];
                        }
                    }
                }
                else
                {
                    text2 = Grupo;
                }

                if(!string.IsNullOrEmpty(text2))
                {
                    return text2;
                }

                return string.Empty;
            }
        }

        public class ResultadoTransformacionSQL : ResultadoSQL
        {
        }

        public class ResultadoSQLExec : ResultadoSQL
        {
            public DataTable Resultados;

            public ResultadoSQLExec()
            {
            }

            public ResultadoSQLExec(ResultadoTransformacionSQL toTransformacion)
            {
                Grupo = toTransformacion.Grupo;
                Ejercicio = toTransformacion.Ejercicio;
                Resultado = toTransformacion.Resultado;
                Consulta = toTransformacion.Consulta;
                Error = toTransformacion.Error;
            }
        }

        public abstract class ResultadoSQL
        {
            public string Grupo;

            public string Ejercicio;

            public bool Resultado;

            public string Error;

            public string Consulta;

            public ResultadoSQL()
            {
                Resultado = true;
                Error = "";
            }
        }

        internal class QueryEjecutable
        {
            private readonly string lcQuery;

            public QueryEjecutable(string tcSql)
            {
                lcQuery = tcSql;
            }

            public DataTable execQuery()
            {
                new SqlDataAdapter();
                DataTable dataTable = new DataTable();
                SqlConnection sqlConnection = new SqlConnection(Conexion);
                SqlDataAdapter obj = new SqlDataAdapter(lcQuery, sqlConnection)
            {
                    SelectCommand =
                {
                    CommandTimeout = 1800
                }
                };
                _SQLOpen(sqlConnection);
                obj.Fill(dataTable);
                sqlConnection.Close();
                return dataTable;
            }
        }

        public class _TableInformationSchema
        {
            public string _DataBase;

            public string _Table;

            public DataTable _INFORMATION_SCHEMA = new DataTable();

            public _TableInformationSchema()
            {
            }

            public _TableInformationSchema(string tcDataBase, string tcTable)
            {
                _DataBase = tcDataBase;
                _Table = tcTable;
                _INFORMATION_SCHEMA = _Get_Schema();
            }

            private DataTable _Get_Schema()
            {
                bool flag = false;
                new SqlCommand();
                DataTable dtTabla = new DataTable();
                _ = string.Empty;
                string empty = string.Empty;
                if(string.IsNullOrEmpty(_DataBase) || string.IsNullOrEmpty(_Table))
                {
                    return new DataTable();
                }

                try
                {
                    SqlConnection sqlConnection = new SqlConnection(Conexion);
                    if(sqlConnection.State == ConnectionState.Open)
                    {
                        flag = true;
                    }
                    else
                    {
                        _SQLOpen(sqlConnection);
                        flag = false;
                    }

                    empty = ParseDatabase(_DataBase);
                    empty = empty.Replace(".dbo.", ".INFORMATION_SCHEMA.");
                    SQLExec("SELECT column_name,data_type,is_nullable,character_maximum_length,column_default,numeric_precision,numeric_scale,ordinal_position,collation_name  FROM " + empty + "COLUMNS  WHERE table_name = " + SQLString(_Table), ref dtTabla);
                    if(!flag)
                    {
                        sqlConnection.Close();
                    }
                }
                catch(Exception toEx)
                {
                    Registrar_Error(toEx);
                    return new DataTable();
                }

                return dtTabla;
            }

            public bool _ExisteCampo(string tcCampo)
            {
                if(_INFORMATION_SCHEMA.Rows.Count == 0)
                {
                    return false;
                }

                return _INFORMATION_SCHEMA.Select("column_name = " + SQLString(tcCampo)).Length != 0;
            }

            public int _AnchuraCampo(string tcCampo)
            {
                int result = 0;
                if(_INFORMATION_SCHEMA.Rows.Count == 0)
                {
                    return 0;
                }

                if(_TipoCampo(tcCampo) == "char" || _TipoCampo(tcCampo) == "varchar")
                {
                    DataRow[] array = _INFORMATION_SCHEMA.Select("column_name = " + SQLString(tcCampo));
                    if(array.Length != 0)
                    {
                        result = Convert.ToInt32(array[0]["character_maximum_length"]);
                    }
                }

                return result;
            }

            public string _TipoCampo(string tcCampo)
            {
                string result = "";
                if(_INFORMATION_SCHEMA.Rows.Count == 0)
                {
                    return "";
                }

                DataRow[] array = _INFORMATION_SCHEMA.Select("column_name = " + SQLString(tcCampo));
                if(array.Length != 0)
                {
                    result = array[0]["data_type"].ToString().ToLower().Trim();
                }

                return result;
            }

            public string _TipoCampoExtended(string tcCampo)
            {
                string text = "";
                if(_INFORMATION_SCHEMA.Rows.Count == 0)
                {
                    return "";
                }

                DataRow[] array = _INFORMATION_SCHEMA.Select("column_name = " + SQLString(tcCampo));
                if(array.Length != 0)
                {
                    text = array[0]["data_type"].ToString().ToLower().Trim();
                    if(!(text == "char"))
                    {
                        if(text == "numeric")
                        {
                            text = text + " (" + array[0]["numeric_precision"].ToString().Trim() + "," + array[0]["numeric_scale"].ToString().Trim() + ")";
                        }
                    }
                    else
                    {
                        text = text + " (" + array[0]["character_maximum_length"].ToString().Trim() + ")";
                    }
                }

                return text;
            }

            public object _ValorPorDefecto(string tcCampo)
            {
                object result = null;
                if(_INFORMATION_SCHEMA.Rows.Count == 0)
                {
                    return null;
                }

                DataRow[] array = _INFORMATION_SCHEMA.Select("column_name = " + SQLString(tcCampo));
                if(array.Length != 0)
                {
                    object obj = array[0]["COLUMN_DEFAULT"];
                    if(obj != null)
                    {
                        string text = obj.ToString().Trim();
                        if(text.Substring(0, 1) == "(")
                        {
                            text = text.Substring(1, text.Length - 2);
                        }

                        if(text.Substring(0, 1) == "(")
                        {
                            text = text.Substring(1, text.Length - 2);
                        }

                        if(text.Substring(0, 1) == "'")
                        {
                            text = text.Substring(1, text.Length - 2);
                        }

                        if(text.ToUpper() != "NULL")
                        {
                            result = ((array[0]["DATA_TYPE"].ToString().ToLower() == "char" || array[0]["DATA_TYPE"].ToString().ToLower() == "varchar") ? text : ((array[0]["DATA_TYPE"].ToString().ToLower() == "int") ? ((object)Convert.ToInt32(text)) : ((array[0]["DATA_TYPE"].ToString().ToLower() == "numeric") ? ((object)Convert.ToDecimal(text)) : ((array[0]["DATA_TYPE"].ToString().ToLower() == "bit") ? ((object)(text != "0")) : ((array[0]["DATA_TYPE"].ToString().ToLower() == "smalldatetime") ? DateTime.Today.ToString(_CustomFormatDate) : ((!(array[0]["DATA_TYPE"].ToString().ToLower() == "datetime")) ? "" : DateTime.Now.ToString()))))));
                        }
                    }
                }

                return result;
            }

            public bool _PermiteNulos(string tcCampo)
            {
                string text = "NO";
                if(_INFORMATION_SCHEMA.Rows.Count == 0)
                {
                    return false;
                }

                DataRow[] array = _INFORMATION_SCHEMA.Select("column_name = " + SQLString(tcCampo));
                if(array.Length != 0)
                {
                    text = Convert.ToString(array[0]["is_nullable"]).ToUpper().Trim();
                }

                return text == "SI";
            }

            public string _Collation(string tcCampo)
            {
                string result = "";
                if(_INFORMATION_SCHEMA.Rows.Count == 0)
                {
                    return "";
                }

                DataRow[] array = _INFORMATION_SCHEMA.Select("column_name = " + SQLString(tcCampo));
                if(array.Length != 0)
                {
                    result = array[0]["collation_name"].ToString().ToLower().Trim();
                }

                return result;
            }
        }

        public class _DBInformationSchema
        {
            public string _DataBase;

            public DataTable _INFORMATION_SCHEMA_TABLES = new DataTable();

            public _DBInformationSchema()
            {
            }

            public _DBInformationSchema(string tcDataBase)
            {
                _DataBase = tcDataBase;
                _INFORMATION_SCHEMA_TABLES = _Get_Tables();
            }

            private DataTable _Get_Tables()
            {
                DataTable dtTabla = new DataTable();
                if(!SQLExec("SELECT TABLE_NAME FROM [" + _DataBase + "].INFORMATION_SCHEMA.TABLES", ref dtTabla) || dtTabla.Rows.Count == 0)
                {
                    dtTabla = new DataTable();
                }

                return dtTabla;
            }

            public bool _ExisteTabla(string tcNombreTabla)
            {
                bool result = false;
                if(string.IsNullOrWhiteSpace(tcNombreTabla))
                {
                    return result;
                }

                if(_INFORMATION_SCHEMA_TABLES.Rows.Count > 0)
                {
                    result = _INFORMATION_SCHEMA_TABLES.Select("TABLE_NAME = '" + tcNombreTabla + "'").Count() > 0;
                }

                return result;
            }

            public bool _AgregarTabla(string tcNombreTabla)
            {
                bool result = true;
                if(string.IsNullOrWhiteSpace(tcNombreTabla))
                {
                    return result;
                }

                if(_INFORMATION_SCHEMA_TABLES.Select("TABLE_NAME = '" + tcNombreTabla + "'").Count() < 1)
                {
                    try
                    {
                        DataRow dataRow = _INFORMATION_SCHEMA_TABLES.NewRow();
                        dataRow["TABLE_NAME"] = tcNombreTabla;
                        _INFORMATION_SCHEMA_TABLES.Rows.Add(dataRow);
                        _INFORMATION_SCHEMA_TABLES.AcceptChanges();
                    }
                    catch(Exception)
                    {
                        result = false;
                    }
                }

                return result;
            }
        }

        public class _ModuloSchema
        {
            public string _Nombre;

            public string _Nombre2;

            public string _Nom_Conex;

            public bool _Instalado;

            public bool _Visible;

            public bool _Activo;

            public int _Solucion;

            public int _SmallProf;
        }

        public class _RegistroConsultas
        {
            public string _Db;

            public string _Tabla;

            public string _Campo;

            public string _Accion;

            public string _Condicion;
        }

        private class _LongitudCampo
        {
            public string _Campo = "";

            public int _Ancho;

            public string _Relleno = "";

            public int _Posicion = -1;

            public int _Lado = 1;

            public DataTable _dtConfig;

            public Dictionary<string, DataTable> _dicIndices = new Dictionary<string, DataTable>();

            public Dictionary<string, DataTable> _dicTablasCampos = new Dictionary<string, DataTable>();

            public bool _log = true;

            public string _cDb = "";

            public string _cTabla = "";

            public string _cCampo = "";

            public string _cDbReal = "";

            public int _Ancho_Old;

            public bool _ConfigPripal;

            public DataTable _dtClave;

            public string _cFiltroDb = "";

            public string _cFiltroClave = "";
        }

        private class Campo
        {
            public string nombre = "";

            public string tipo = "";

            public string tipobase = "";

            public bool nulo;

            public int longitud;

            public string defecto = "";

            public int precision;

            public int decimales;

            public int posicion;

            public string basedatos = "";

            public string tabla = "";
        }

        private class CompareBD
        {
            public string bdorigen = "";

            public string bdlogicorigen = "";

            public string bddestino = "";

            public string bdlogicdestino = "";

            public string tabla = "";

            public string ficheroLog = "";

            public string MsgError = "";

            public bool error;
        }

        internal static readonly string _CustomFormatDate;

        private static List<string> _cExclusionWords;

        private static List<string> _cAgregateFunctions;

        public static bool Registro_Errores;

        public static int Registro_Log_Consultas;

        public static string Usuario_EW;

        public static string Ejercicio_EW;

        public static string Error_Message;

        public static Exception Error_Message_Exception;

        public static string Conexion;

        public static string DbComunes;

        public static Dictionary<string, string> _oAliasDB;

        private static Dictionary<string, Dictionary<string, object>> _oConexionDB;

        public static string CurrentAlias;

        public static SqlConnection _oPersistConnection;

        public static Dictionary<string, _ModuloSchema> _dicCacheModulos;

        public static Dictionary<int, _RegistroConsultas> _dicRegistroConsultas;

        private static _LongitudCampo _oCambioLong;

        private static bool _lEurowinSys_Checked;

        public static Dictionary<string, string> _oAliasDBEjer;

        public static Dictionary<string, string> _oEjerActualAnterior;

        public static string _RutaBaseDatos;

        public static string UsuarioConex;

        public static string ServerConex;

        public static string PasswordConex;

        public static bool AuthWin;

        public static string VersionSQLServer;

        public static bool _ExternalConnect;

        private static CompareBD oCompareEst;

        internal static Dictionary<int, Transac> _oTrans;

        private static bool _lWritingAnalisis;

        private static string _cVersionMotor;

        private static bool _oPersist;

        private static DBQueryManager _QueryManager;

        private static Dictionary<string, InfoGrupo> _oGrupos;

        private static List<string> _oDataBases;

        private static int _nTimeCache;

        private static bool _lRegistro_Consulta_Recursivo;

        public static Dictionary<string, _TableInformationSchema> _dicCacheSchema;

        private static DataTable preloadSchemas_TablaModelo;

        public static Dictionary<string, _DBInformationSchema> _dicCacheTables;

        public static Dictionary<string, object> _VarGlob;

        public static string _VersionMotor => _cVersionMotor;

        public static bool _Persist
        {
            get
            {
                return _oPersist;
            }
            set
            {
                if(!value)
                {
                    if(_oPersistConnection != null && _oPersistConnection.State == ConnectionState.Open)
                    {
                        _oPersistConnection.Close();
                    }

                    _oPersistConnection = null;
                    _QueryManager.RestablecerTipoConsultaAnterior();
                }
                else
                {
                    _QueryManager.CambiarTipoConsultasA(eTipoQuery.Persistente);
                }

                _oPersist = value;
            }
        }

        private static List<string> _DataBases
        {
            get
            {
                if(_oDataBases == null)
                {
                    DataTable dtTabla = new DataTable();
                    SQLExec("SELECT Name FROM sys.databases", ref dtTabla);
                    _oDataBases = (from loRow in dtTabla.AsEnumerable()
                                   select Convert.ToString(loRow["Name"])).ToList();
                }

                return _oDataBases;
            }
        }

        public static event _SQLEXEC_Before_Delegate _SQLEXEC_Before;

        public static event _SQLEXEC_Before_Parameters_Delegate _SQLEXEC_Before_Parameters;

        public static event _SQLEXEC_After_Delegate _SQLEXEC_After;

        static ReplicatedDB()
        {
            _CustomFormatDate = "dd'/'MM'/'yyyy";
            _cExclusionWords = new List<string> { "DROP", "TRUNCATE", "DELETE", "UPDATE", "INSERT" };
            _cAgregateFunctions = new List<string> { "SUM(", "COUNT(", "MIN(", "MAX(", "AVG(" };
            Registro_Errores = !Debugger.IsAttached;
            Registro_Log_Consultas = -1;
            Usuario_EW = string.Empty;
            Ejercicio_EW = string.Empty;
            Error_Message = string.Empty;
            Error_Message_Exception = null;
            Conexion = string.Empty;
            DbComunes = string.Empty;
            _oAliasDB = new Dictionary<string, string>();
            _oConexionDB = new Dictionary<string, Dictionary<string, object>>();
            CurrentAlias = string.Empty;
            _oPersistConnection = null;
            _dicCacheModulos = new Dictionary<string, _ModuloSchema>();
            _dicRegistroConsultas = new Dictionary<int, _RegistroConsultas>();
            _lEurowinSys_Checked = false;
            _oAliasDBEjer = new Dictionary<string, string>();
            _oEjerActualAnterior = new Dictionary<string, string>();
            _RutaBaseDatos = "";
            UsuarioConex = string.Empty;
            ServerConex = string.Empty;
            PasswordConex = string.Empty;
            AuthWin = false;
            VersionSQLServer = string.Empty;
            _ExternalConnect = false;
            _oTrans = new Dictionary<int, Transac>();
            _lWritingAnalisis = false;
            _cVersionMotor = "";
            _oPersist = false;
            _QueryManager = new DBQueryManager();
            _oGrupos = new Dictionary<string, InfoGrupo>();
            _oDataBases = null;
            _nTimeCache = -1;
            _lRegistro_Consulta_Recursivo = false;
            _dicCacheSchema = new Dictionary<string, _TableInformationSchema>();
            preloadSchemas_TablaModelo = null;
            _dicCacheTables = new Dictionary<string, _DBInformationSchema>();
            _VarGlob = new Dictionary<string, object>();
        }

        private static DataTable SQLInformationSchema(string tcDatabaseLogica, string tcTabla)
        {
            return _TablesInformationSchema(tcDatabaseLogica, tcTabla)._INFORMATION_SCHEMA;
        }

        private static string ObtenerTipoCampoEstandar(string tcTipoCampoSqlServer)
        {
            string result = "";
            switch(tcTipoCampoSqlServer)
            {
                case "bit":
                    result = "logico";
                    break;
                case "char":
                case "varchar":
                case "text":
                case "nchar":
                case "nvarchar":
                case "ntext":
                    result = "caracter";
                    break;
                case "date":
                case "datetimeoffset":
                case "datetime2":
                case "smalldatetime":
                case "datetime":
                case "time":
                    result = "fecha";
                    break;
                case "bigint":
                case "numeric":
                case "smallint":
                case "decimal":
                case "smallmoney":
                case "int":
                case "tinyint":
                case "float":
                case "real":
                    result = "numerico";
                    break;
            }

            return result;
        }

        private static string ObtenerTipoCampoEstandarExtended(string tcTipoCampoSqlServer)
        {
            if(tcTipoCampoSqlServer.Contains("bit"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("bit", "Lógico");
            }

            if(tcTipoCampoSqlServer.Contains("char"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("char", "Carácter");
            }

            if(tcTipoCampoSqlServer.Contains("varchar"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("varchar", "Carácter");
            }

            if(tcTipoCampoSqlServer.Contains("text"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("text", "Texto");
            }

            if(tcTipoCampoSqlServer.Contains("nchar"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("nchar", "Carácter");
            }

            if(tcTipoCampoSqlServer.Contains("nvarchar"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("nvarchar", "Carácter");
            }

            if(tcTipoCampoSqlServer.Contains("ntext"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("ntext", "Texto");
            }

            if(tcTipoCampoSqlServer.Contains("datetimeoffset"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("datetimeoffset", "Fecha");
            }

            if(tcTipoCampoSqlServer.Contains("datetime2"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("datetime2", "Fecha");
            }

            if(tcTipoCampoSqlServer.Contains("smalldatetime"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("smalldatetime", "Fecha");
            }

            if(tcTipoCampoSqlServer.Contains("datetime"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("datetime", "Fecha y hora");
            }

            if(tcTipoCampoSqlServer.Contains("time"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("time", "Fecha");
            }

            if(tcTipoCampoSqlServer.Contains("date"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("date", "Fecha");
            }

            if(tcTipoCampoSqlServer.Contains("bigint"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("bigint", "Entero");
            }

            if(tcTipoCampoSqlServer.Contains("numeric"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("numeric", "Numérico");
            }

            if(tcTipoCampoSqlServer.Contains("smallint"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("smallint", "Entero");
            }

            if(tcTipoCampoSqlServer.Contains("decimal"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("decimal", "Numérico");
            }

            if(tcTipoCampoSqlServer.Contains("smallmoney"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("smallmoney", "Numérico");
            }

            if(tcTipoCampoSqlServer.Contains("int"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("int", "Entero");
            }

            if(tcTipoCampoSqlServer.Contains("tinyint"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("tinyint", "Entero");
            }

            if(tcTipoCampoSqlServer.Contains("float"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("float", "Numérico");
            }

            if(tcTipoCampoSqlServer.Contains("real"))
            {
                tcTipoCampoSqlServer = tcTipoCampoSqlServer.Replace("real", "Numérico");
            }

            return tcTipoCampoSqlServer;
        }

        private static bool SQLComprobarConexion(string tcConexion)
        {
            string text = "";
            string text2 = "";
            int num = 0;
            int num2 = 0;
            SqlConnection sqlConnection = null;
            Exception ex = null;
            text = tcConexion;
            num = tcConexion.IndexOf("Timeout");
            if(num > 0)
            {
                text2 = tcConexion.Substring(num);
                num2 = text2.IndexOf(";");
                text2 = text2.Substring(0, num2);
                text = tcConexion.Replace(text2, "Timeout=60");
            }

            try
            {
                sqlConnection = new SqlConnection(text);
                sqlConnection.Open();
            }
            catch(InvalidOperationException ex2)
            {
                ex = ex2;
            }
            catch(SqlException ex3)
            {
                ex = ex3;
            }
            catch(Exception ex4)
            {
                ex = ex4;
            }
            finally
            {
                sqlConnection?.Close();
            }

            if(ex != null)
            {
                Registrar_Error(ex);
                return false;
            }

            return true;
        }

        private static string DB_LeerConfigIni(string tcEtiqueta, string tcRutaIni = "", string tcFicheroIni = "")
        {
            string empty = string.Empty;
            string text = string.Empty;
            bool flag = false;
            if(string.IsNullOrWhiteSpace(tcEtiqueta))
            {
                return "";
            }

            try
            {
                tcEtiqueta = tcEtiqueta.Trim();
                empty = ((!string.IsNullOrWhiteSpace(tcRutaIni)) ? tcRutaIni : _GetVariable("wc_pathinicio").ToString());
                empty = System.IO.Path.Combine(empty, (!string.IsNullOrWhiteSpace(tcFicheroIni)) ? tcFicheroIni : "config.ini");
                if(File.Exists(empty))
                {
                    StreamReader streamReader = File.OpenText(empty);
                    while((text = streamReader.ReadLine()) != null)
                    {
                        text = text.Trim();
                        if(text == tcEtiqueta)
                        {
                            text = streamReader.ReadLine().Trim();
                            flag = true;
                            break;
                        }
                    }

                    streamReader.Close();
                }

                if(!flag)
                {
                    text = "";
                }
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
                text = "";
            }

            return text;
        }

        private static void Control_Modo_Log_Consultas()
        {
            if(Registro_Log_Consultas == -1 && Convert.ToBoolean(_GetVariable("_Cargados_Diccionarios")) && !string.IsNullOrWhiteSpace(_GetVariable("wc_pathinicio").ToString().Trim()) && Directory.Exists(_GetVariable("wc_pathinicio").ToString().Trim()))
            {
                string toValor = Convert.ToString(DB_LeerConfigIni("[SQL_LOG]")).ToUpper().Trim();
                _SetVariable("wc_sql_log", toValor);
                if(!string.IsNullOrWhiteSpace(Convert.ToString(_GetVariable("wc_usuario"))))
                {
                    Registro_Log_Consultas = ((Convert.ToString(_GetVariable("wc_sql_log")).Trim().ToUpper() == "SI") ? 1 : 0);
                }
            }
        }

        private static void ObtenerPila(ref string tcPila, ref string tcProces, ref int tnLinea, bool tlModoConsulta = false)
        {
            StackTrace stackTrace = new StackTrace(1, fNeedFileInfo: true);
            for(int i = 0; i < stackTrace.FrameCount; i++)
            {
                StackFrame frame = stackTrace.GetFrame(i);
                tcPila = tcPila + "Método: " + frame.GetMethod()?.ToString() + Environment.NewLine;
                tcPila = tcPila + "Línea: " + frame.GetFileLineNumber() + Environment.NewLine;
                Uri.UnescapeDataString(new UriBuilder(Assembly.GetAssembly(frame.GetType()).CodeBase).Path);
                tcPila = tcPila + "Libreria: " + frame.GetMethod().Module?.ToString() + Environment.NewLine;
                tcPila = tcPila + "Classe: " + frame.GetMethod().ReflectedType?.ToString() + ";" + Environment.NewLine;
                if(i == 3)
                {
                    tcProces = frame.GetMethod().ToString().Trim();
                    tnLinea = frame.GetFileLineNumber();
                }

                if(tlModoConsulta)
                {
                    tnLinea = 9999;
                }
            }
        }

        private static bool Escribir_En_Log_Analisis(int tnAplica, string tcTerminal, string tcUsuario, string tcTipo, string tcLibreria = "", string tcFitchero = "", decimal tnTiempoRelativo = 0m, decimal tnTiempoAcumulado = 0m, string tcMensaje = "", string tcConsulta = "", string tcPila = "", bool tlThread = false, string tcInfoThread = "")
        {
            if(tcMensaje.Length > 220)
            {
                tcMensaje = tcMensaje.Substring(0, 220);
            }

            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            bool result = SQLExec("insert into " + SQLDatabase("eurowinsys", "log_analisis") + " (aplica, terminal, usuari, tempshora, tipo, libreria, fichero, tempsrelatiu, tempsacumulat, missatge, consulta, pila, hilo, infohilo) VALUES(" + SQLString(tnAplica) + "," + SQLString(tcTerminal) + "," + SQLString(tcUsuario) + ", getdate(), " + SQLString(tcTipo) + "," + SQLString(tcLibreria) + ", " + SQLString(tcFitchero) + ", " + tnTiempoRelativo + "," + tnTiempoAcumulado + ", " + SQLString(tcMensaje) + ", " + SQLString(tcConsulta) + "," + SQLString(tcPila) + "," + SQLString(tlThread) + "," + SQLString(tcInfoThread) + ")");
            SQLChangeConnection(currentAlias);
            _lWritingAnalisis = false;
            return result;
        }

        public static void _Cargar_Alias_DB(string tcComunes)
        {
            DataTable dtTabla = new DataTable();
            string empty = string.Empty;
            try
            {
                _oAliasDB.Clear();
                _oAliasDBEjer.Clear();
                _oEjerActualAnterior.Clear();
                _dicCacheSchema.Clear();
                _oAliasDB.Add("COMUNES", "[" + tcComunes + "].dbo.");
                _oAliasDB.Add("EUROWINSYS", "[eurowinsys].dbo.");
                SQLExec("SELECT * FROM " + SQLDatabase("COMUNES", "EJERCICI"), ref dtTabla);
                foreach(DataRow row in dtTabla.Rows)
                {
                    empty = row["conexion"].ToString().ToLower().Trim();
                    if(!string.IsNullOrEmpty(empty))
                    {
                        empty = "[" + empty + "].dbo.";
                        if(Convert.ToBoolean(row["predet"]))
                        {
                            _oAliasDB.Add("2024XP", empty);
                            Ejercicio_EW = row["any"].ToString().Trim();
                        }

                        if(!_oAliasDB.ContainsKey(row["any"].ToString().Trim()))
                        {
                            _oAliasDB.Add(row["any"].ToString().Trim(), empty);
                        }

                        if(!_oAliasDBEjer.ContainsKey(row["any"].ToString().Trim()))
                        {
                            _oAliasDBEjer.Add(row["any"].ToString().Trim(), empty);
                        }

                        if(!_oEjerActualAnterior.ContainsKey(row["any"].ToString().Trim()))
                        {
                            _oEjerActualAnterior.Add(row["any"].ToString().Trim(), row["anterior"].ToString().Trim());
                        }
                    }
                }

                if(!_SQLExisteTablaBBDD("EUROWINSYS", "GRUPOSEMP"))
                {
                    return;
                }

                dtTabla = new DataTable();
                SQLExec("SELECT * FROM " + SQLDatabase("EUROWINSYS", "gruposemp"), ref dtTabla);
                foreach(DataRow row2 in dtTabla.Rows)
                {
                    string text = "COMU" + row2["codigo"].ToString().Trim();
                    _oAliasDB.Add(text, "[" + text.ToLower() + "].dbo.");
                }
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
            }
        }

        private static void _Cargar_Alias_DB_Modulos()
        {
            new DataTable();
            string empty = string.Empty;
            try
            {
                foreach(KeyValuePair<string, _ModuloSchema> dicCacheModulo in _dicCacheModulos)
                {
                    empty = dicCacheModulo.Value._Nom_Conex.ToString().ToLower().Trim();
                    if(!string.IsNullOrEmpty(empty))
                    {
                        empty = "[" + empty + "].dbo.";
                        if(!_oAliasDB.ContainsKey(dicCacheModulo.Key.ToString().ToUpper().Trim()))
                        {
                            _oAliasDB.Add(dicCacheModulo.Key.ToString().ToUpper().Trim(), empty);
                        }
                    }
                }
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
            }
        }

        private static void _Carga_Modulos()
        {
            DataTable dtTabla = new DataTable();
            bool num = Convert.ToBoolean(_GetVariable("wl_sage50"));
            string text = _GetVariable("wc_iniservidor").ToString().ToLower();
            string text2 = "";
            string text3 = "";
            string text4 = "";
            string text5 = "";
            _dicCacheModulos.Clear();
            text5 = ((!num) ? ("SELECT nombre, nombre2, nom_conex, " + SQLTrue() + " as activo, " + SQLTrue() + " as visible, " + SQLTrue() + " as instalado, 0 as solucion, 0 as smallprof ") : "SELECT nombre, nombre2, nom_conex, activo, visible, instalado, solucion, smallprof ");
            text5 = text5 + "FROM " + SQLDatabase("COMUNES", "MODULOS") + " WHERE nombre != 'EWTOOLS' " + text4 + "ORDER BY nombre ";
            SQLExec(text5, ref dtTabla);
            List<string> list = new List<string>();
            if(!string.IsNullOrWhiteSpace(text))
            {
                text2 = System.IO.Path.Combine(text, "modulos");
                if(!Directory.Exists(text2))
                {
                    Directory.CreateDirectory(text2);
                }

                if(Directory.Exists(text2))
                {
                    list = Directory.GetDirectories(text2).ToList().ConvertAll((string d) => d.ToLower());
                }
            }

            foreach(DataRow row in dtTabla.Rows)
            {
                text3 = row["nombre"].ToString().ToLower().Trim();
                bool flag = true;
                if(!string.IsNullOrWhiteSpace(text) && !Debugger.IsAttached && !list.Contains(System.IO.Path.Combine(text2, text3)))
                {
                    flag = false;
                }

                if(flag)
                {
                    _ModuloSchema moduloSchema = new _ModuloSchema();
                    moduloSchema._Nom_Conex = row["nom_conex"].ToString().ToLower().Trim();
                    moduloSchema._Nombre = text3;
                    moduloSchema._Nombre2 = row["nombre2"].ToString().ToLower().Trim();
                    moduloSchema._Activo = Convert.ToBoolean(row["activo"]);
                    moduloSchema._Instalado = Convert.ToBoolean(row["instalado"]);
                    moduloSchema._Visible = Convert.ToBoolean(row["visible"]);
                    moduloSchema._Solucion = Convert.ToInt16(row["solucion"]);
                    moduloSchema._SmallProf = Convert.ToInt16(row["smallprof"]);
                    if(!_dicCacheModulos.ContainsKey(moduloSchema._Nombre))
                    {
                        _dicCacheModulos.Add(moduloSchema._Nombre, moduloSchema);
                    }
                }
                else if(!Debugger.IsAttached)
                {
                    Registrar_Error(new Exception("No existe la carpeta " + text3 + " dentro de la carpeta MODULOS del servidor. No se cargará el add-on"));
                }
            }
        }

        private static void _Carga_Modulos_Tablas()
        {
            new DataTable();
            foreach(KeyValuePair<string, _ModuloSchema> dicCacheModulo in _dicCacheModulos)
            {
                _DBsInformationSchema(dicCacheModulo.Value._Nom_Conex);
            }
        }

        private static void _Carga_Registro_Consultas()
        {
            DataTable dtTabla = new DataTable();
            _dicRegistroConsultas.Clear();
            SQLExec("SELECT id, db, tabla, campo, accion, condicion  FROM " + SQLDatabase("COMUNES", "REG_CONS") + " Order by id ", ref dtTabla);
            foreach(DataRow row in dtTabla.Rows)
            {
                _RegistroConsultas registroConsultas = new _RegistroConsultas();
                string value = "";
                if(_oAliasDB.TryGetValue(Convert.ToString(row["db"]).Trim(), out value) && !string.IsNullOrEmpty(value))
                {
                    registroConsultas._Db = value.ToLower();
                    registroConsultas._Tabla = Convert.ToString(row["tabla"]).Trim().ToLower();
                    registroConsultas._Campo = Convert.ToString(row["campo"]).Trim().ToLower();
                    registroConsultas._Accion = Convert.ToString(row["accion"]).Trim().ToLower();
                    registroConsultas._Condicion = Convert.ToString(row["condicion"]).Trim().ToLower();
                    if(!_dicRegistroConsultas.ContainsKey(Convert.ToInt32(row["id"])))
                    {
                        _dicRegistroConsultas.Add(Convert.ToInt32(row["id"]), registroConsultas);
                    }
                }
            }
        }

        private static string Crear_Select_List(string[] taCampos, ref string tcGroupBy)
        {
            string text = "";
            string text2 = "";
            bool flag = false;
            string arg = "";
            foreach(string text3 in taCampos)
            {
                if(_Contiene_FuncionesDeAgregado(text3))
                {
                    flag = true;
                    text += $"{arg} {text3}";
                }
                else
                {
                    text += $"{arg} [{text3}]";
                    text2 += $"{arg} [{text3}] ";
                }

                arg = " , ";
            }

            if(flag && !string.IsNullOrWhiteSpace(text2))
            {
                tcGroupBy = " GROUP BY " + text2;
            }

            return text;
        }

        private static bool _Contiene_FuncionesDeAgregado(string tcTexto)
        {
            if(!string.IsNullOrWhiteSpace(tcTexto) && _cAgregateFunctions.Any((string lcWord) => tcTexto.ToUpper().Contains(lcWord.ToUpper())))
            {
                return true;
            }

            return false;
        }

        private static Dictionary<string, object> _SQLValor(string tcTabla, string[] tcWhere, object[] tcClave, string tcDatabase = "2024XP")
        {
            string empty = string.Empty;
            bool flag = true;
            DataTable dtTabla = new DataTable();
            Dictionary<string, object> dictionary = new Dictionary<string, object>();
            new object();
            if(tcWhere.Length != tcClave.Length)
            {
                Error_Message = "Debe indicar el mismo número de elementos en el parámetro tcWhere que en tcClave";
                return null;
            }

            if(tcWhere.Length == 0 || tcClave.Length == 0)
            {
                Error_Message = "Debe indicar algún elemento en los parámetros tcWhere y tcClave";
                return null;
            }

            try
            {
                empty = "SELECT * FROM " + SQLDatabase(tcDatabase, tcTabla);
                if(tcWhere.Length == 1 && tcWhere[0] == string.Empty)
                {
                    flag = false;
                }

                if(flag)
                {
                    empty += " WHERE ";
                    for(int i = 0; i < tcWhere.Length; i++)
                    {
                        if(i > 0)
                        {
                            empty += " AND ";
                        }

                        empty = empty + tcWhere[i] + " = " + SQLString(tcClave[i]);
                    }
                }

                if(!SQLExec(empty, ref dtTabla))
                {
                    return null;
                }

                for(int j = 0; j < dtTabla.Columns.Count; j++)
                {
                    if(dtTabla.Rows.Count <= 0)
                    {
                        dictionary.Add(dtTabla.Columns[j].ColumnName.ToLower(), _Valor_Defecto(dtTabla.Columns[j].DataType));
                    }
                    else
                    {
                        dictionary.Add(dtTabla.Columns[j].ColumnName.ToLower(), dtTabla.Rows[0][j]);
                    }
                }

                return dictionary;
            }
            catch(Exception toEx)
            {
                dtTabla = null;
                dictionary = null;
                Registrar_Error(toEx);
                return null;
            }
        }

        private static Dictionary<string, object> _SQLValor(string tcTabla, string[] tcCampo, string[] tcWhere, object[] tcClave, string tcDatabase = "2024XP")
        {
            string empty = string.Empty;
            bool flag = true;
            DataTable dtTabla = new DataTable();
            Dictionary<string, object> dictionary = new Dictionary<string, object>();
            new object();

            MessageBox.Show(
                "AT:"
                + "\n\n" +
                "ReplicatedDB.cs"
                + "\n\n" +
                "On:"
                + "\n\n" +
                "_SQLValor(string tcTabla, string[] tcCampo, string[] tcWhere, object[] tcClave, string tcDatabase = \"2024XP\")"
            );

            if(tcWhere.Length != tcClave.Length)
            {
                Error_Message = "Debe indicar el mismo número de elementos en el parámetro tcWhere que en tcClave";
                return null;
            }

            if(tcWhere.Length == 0 || tcClave.Length == 0)
            {
                Error_Message = "Debe indicar algún elemento en los parámetros tcWhere y tcClave";
                return null;
            }

            try
            {
                string text = "*";
                foreach(string arg in tcCampo)
                {
                    text = ((!(text == "*")) ? (text + $", [{arg}]") : $" [{arg}]");
                }
                /////////////////////////////////////
                /////////////////////////////////////
                /////////////////////////////////////
                /////////////////////////////////////
                /////////////////////////////////////
                /////////////////////////////////////
                /////////////////////////////////////
                /////////////////////////////////////
                /////////////////////////////////////
                ///


                MessageBox.Show(
                    "AT:"
                    + "\n\n" +
                    "ReplicatedDB.cs"
                    + "\n\n" +
                    "On:"
                    + "\n\n" +
                    "_SQLValor(string tcTabla, string[] tcCampo, string[] tcWhere, object[] tcClave, string tcDatabase = \"2024XP\")"
                    + "\n\n" +
                    "BEFORE:"
                    + "\n\n" +
                    "empty = $\"SELECT {text} FROM {SQLDatabase(tcDatabase, tcTabla)}"
                );

                empty = $"SELECT {text} FROM {SQLDatabase(tcDatabase, tcTabla)}";



                MessageBox.Show(
                    "AT:"
                    + "\n\n" +
                    "ReplicatedDB.cs"
                    + "\n\n" +
                    "On:"
                    + "\n\n" +
                    "_SQLValor(string tcTabla, string[] tcCampo, string[] tcWhere, object[] tcClave, string tcDatabase = \"2024XP\")"
                    + "\n\n" +
                    "BEFORE:"
                    + "\n\n" +
                    "empty = $\"SELECT {text} FROM if(tcWhere.Length == 1 && tcWhere[0] == string.Empty)"
                );

                if(tcWhere.Length == 1 && tcWhere[0] == string.Empty)
                {
                    flag = false;
                }

                if(flag)
                {
                    empty += " WHERE ";
                    for(int j = 0; j < tcWhere.Length; j++)
                    {
                        if(j > 0)
                        {
                            empty += " AND ";
                        }

                        empty = empty + tcWhere[j] + " = " + SQLString(tcClave[j]);
                    }
                }




                MessageBox.Show(
                    "AT:"
                    + "\n\n" +
                    "ReplicatedDB.cs"
                    + "\n\n" +
                    "On:"
                    + "\n\n" +
                    "_SQLValor(string tcTabla, string[] tcCampo, string[] tcWhere, object[] tcClave, string tcDatabase = \"2024XP\")"
                    + "\n\n" +
                    "BEFORE:"
                    + "\n\n" +
                    "if(!SQLExec(empty, ref dtTabla))"
                );

                if(!SQLExec(empty, ref dtTabla))
                {
                    return null;
                }

                for(int k = 0; k < dtTabla.Columns.Count; k++)
                {
                    if(dtTabla.Rows.Count <= 0)
                    {
                        dictionary.Add(dtTabla.Columns[k].ColumnName.ToLower(), _Valor_Defecto(dtTabla.Columns[k].DataType));
                    }
                    else
                    {
                        dictionary.Add(dtTabla.Columns[k].ColumnName.ToLower(), dtTabla.Rows[0][k]);
                    }
                }

                return dictionary;
            }
            catch(Exception toEx)
            {
                dtTabla = null;
                dictionary = null;
                Registrar_Error(toEx);
                return null;
            }
        }

        private static object _Valor_Defecto(Type toTipo)
        {
            object result = null;
            try
            {
                switch(toTipo.ToString().Trim().ToLower())
                {
                    case "system.string":
                        result = string.Empty;
                        break;
                    case "system.boolean":
                        result = false;
                        break;
                    case "system.datetime":
                        result = DateTime.MinValue;
                        break;
                    case "system.decimal":
                    case "system.double":
                    case "system.int16":
                    case "system.int32":
                    case "system.int64":
                    case "system.uint16":
                    case "system.uint32":
                    case "system.uint64":
                        result = 0;
                        break;
                }
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
            }

            return result;
        }

        internal static void _SQLOpen(SqlConnection tConexion, int tnReintentos = 0)
        {
            bool flag = true;
            try
            {
                switch(tConexion.State)
                {
                    case ConnectionState.Connecting:
                        Thread.Sleep(10);
                        if(tConexion.State != ConnectionState.Open)
                        {
                            tConexion.Open();
                        }

                        break;
                    default:
                        tConexion.Open();
                        break;
                    case ConnectionState.Open:
                        break;
                }

                string commandText = "set ansi_nulls off set ansi_defaults off set ansi_padding on set dateformat dmy set quoted_identifier on set implicit_transactions off set transaction isolation level serializable ";
                SqlCommand sqlCommand = new SqlCommand();
                sqlCommand.CommandText = commandText;
                sqlCommand.Connection = tConexion;
                if(tConexion.State == ConnectionState.Open)
                {
                    sqlCommand.ExecuteNonQuery();
                }
                else
                {
                    flag = false;
                }

                if((sqlCommand.Connection.State != ConnectionState.Open || !flag) && tnReintentos < 5)
                {
                    sqlCommand.Connection.Close();
                    tnReintentos++;
                    _SQLOpen(tConexion, tnReintentos);
                }
            }
            catch(Exception toEx)
            {
                if(tnReintentos < 5)
                {
                    tConexion.Close();
                    tnReintentos++;
                    _SQLOpen(tConexion, tnReintentos);
                }
                else
                {
                    Registrar_Error(toEx);
                }
            }
        }

        private static string NuevaBd(string tcPrefijoNombreDb, int tnLongNombreDb, string tcMes = "", DataTable tdtBdsExcluir = null)
        {
            string text = "";
            string text2 = "";
            string text3 = "";
            int num = 0;
            int num2 = 0;
            int num3 = 0;
            int num4 = 0;
            DataTable dtTabla = new DataTable();
            SQLExec("SELECT name FROM [master].sys.databases", ref dtTabla);
            if(tdtBdsExcluir != null && tdtBdsExcluir != null && tdtBdsExcluir.Columns.Contains("name"))
            {
                foreach(DataRow row in tdtBdsExcluir.Rows)
                {
                    dtTabla.ImportRow(row);
                }
            }

            num3 = 1;
            num2 = tnLongNombreDb - tcPrefijoNombreDb.Length;
            if(string.IsNullOrWhiteSpace(tcMes))
            {
                text2 = "1";
            }
            else
            {
                DateTime now = DateTime.Now;
                num4 = Convert.ToInt32(now.TimeOfDay.TotalSeconds);
                num = now.Minute + num4 % 30;
                text2 = Convert.ToString((char)(65 + num % 25));
                text3 = tcMes + text2;
            }

            text = tcPrefijoNombreDb + (tcMes + text2).PadLeft(num2, '0');
            int num5 = 0;
            int num6 = 0;
            num6 = ((!string.IsNullOrWhiteSpace(tcMes)) ? 786 : 10002);
            while(dtTabla.Select("name=" + SQLString(text)).Count() != 0)
            {
                num5++;
                if(num5 > num6)
                {
                    throw new Exception("No se ha podido obtener el nombre de una nueva base de datos de " + (string.IsNullOrWhiteSpace(tcMes) ? "COMUNES" : "2024XP"));
                }

                if(!string.IsNullOrWhiteSpace(tcMes))
                {
                    text3 = incrementarSufijo(text3);
                    text = tcPrefijoNombreDb + text3.PadLeft(num2, '0');
                }
                else
                {
                    num3++;
                    text2 = Convert.ToString(num3);
                    text = tcPrefijoNombreDb + (tcMes + text2).PadLeft(num2, '0');
                }
            }

            return text;
        }

        private static string incrementarSufijo(string tcSufijo)
        {
            string text = "";
            char c = ' ';
            if(tcSufijo == "ZZ")
            {
                return "AA";
            }

            if(tcSufijo.Substring(1, 1) == "Z")
            {
                c = char.Parse(tcSufijo.Substring(0, 1));
                return (char)(c + 1) + "A";
            }

            c = char.Parse(tcSufijo.Substring(1, 1));
            return tcSufijo.Substring(0, 1) + (char)(c + 1);
        }

        public static bool _EsPenultimoEjercicio()
        {
            return _oAliasDBEjer.Where((KeyValuePair<string, string> f) => Convert.ToInt32(f.Key) > Convert.ToInt32(Ejercicio_EW)).Count() == 1;
        }

        public static string _SiguienteEjercicio()
        {
            string result = "";
            if(_oEjerActualAnterior.ContainsValue(Ejercicio_EW))
            {
                result = _oEjerActualAnterior.Where((KeyValuePair<string, string> f) => f.Value == Ejercicio_EW).ElementAt(0).Key;
            }

            return result;
        }

        public static string _AnteriorEjercicio()
        {
            string result = "";
            if(_oEjerActualAnterior.ContainsKey(Ejercicio_EW))
            {
                result = _oEjerActualAnterior[Ejercicio_EW];
            }

            return result;
        }

        public static bool _DbIndices(string tcNombreBd, ref DataTable tdtIndices)
        {
            if(string.IsNullOrWhiteSpace(tcNombreBd))
            {
                Error_Message = "No se ha recibido el nombre de la base de datos sobre la cual trabajar.";
                return false;
            }

            tcNombreBd = tcNombreBd.Trim();
            return SQLExec("SELECT t.name as tabla,  i.name as indice, i.type as indice_tipo, i.type_desc as indice_tipo_desc, i.is_primary_key as indice_primario FROM  [" + tcNombreBd + "].sys.indexes i INNER JOIN  [" + tcNombreBd + "].sys.tables t ON t.object_id = i.object_id WHERE i.type>0  order by t.name, i.is_primary_key desc, i.name ", ref tdtIndices);
        }

        public static bool _GetCreateTableSql(DataTable toTable, string tcTableName)
        {
            new List<string>();
            StringBuilder stringBuilder = new StringBuilder();
            int num = 0;
            stringBuilder.AppendFormat("CREATE TABLE {0} (", tcTableName);
            string lcColumnName;
            for(int i = 0; i < toTable.Columns.Count; i++)
            {
                lcColumnName = toTable.Columns[i].ColumnName;
                stringBuilder.AppendFormat("\n\t[{0}]", lcColumnName);
                switch(toTable.Columns[i].DataType.ToString().ToUpper())
                {
                    case "SYSTEM.INT16":
                        stringBuilder.Append(" smallint");
                        break;
                    case "SYSTEM.INT32":
                        stringBuilder.Append(" int");
                        break;
                    case "SYSTEM.INT64":
                        stringBuilder.Append(" bigint");
                        break;
                    case "SYSTEM.DATETIME":
                        stringBuilder.Append(" datetime");
                        (from ldRow in toTable.AsEnumerable()
                         where !ldRow.IsNull(lcColumnName) && (Convert.ToDateTime(ldRow[lcColumnName]) < SqlDateTime.MinValue.Value || Convert.ToDateTime(ldRow[lcColumnName]) > SqlDateTime.MaxValue.Value)
                         select ldRow).ToList().ForEach(delegate (DataRow f) {
                             f[lcColumnName] = DBNull.Value;
                         });
                        break;
                    case "SYSTEM.STRING":
                    {
                        EnumerableRowCollection<int> source2 = from ldRow in toTable.AsEnumerable()
                                                               where !ldRow.IsNull(lcColumnName)
                                                               select ldRow.Field<string>(lcColumnName).Length;
                        num = ((source2.Count() > 0) ? source2.Max() : 0);
                        stringBuilder.AppendFormat(" nvarchar({0}) COLLATE Modern_Spanish_CS_AI", (num > 0) ? num : 255);
                        break;
                    }
                    case "SYSTEM.SINGLE":
                        stringBuilder.Append(" single");
                        break;
                    case "SYSTEM.DOUBLE":
                        stringBuilder.Append(" double");
                        break;
                    case "SYSTEM.DECIMAL":
                        stringBuilder.AppendFormat(" decimal(18, 6)");
                        break;
                    case "SYSTEM.BOOLEAN":
                        stringBuilder.AppendFormat(" bit");
                        break;
                    default:
                    {
                        EnumerableRowCollection<int> source = from ldRow in toTable.AsEnumerable()
                                                              where !ldRow.IsNull(lcColumnName)
                                                              select ldRow.Field<string>(lcColumnName).Length;
                        num = ((source.Count() > 0) ? source.Max() : 0);
                        stringBuilder.AppendFormat(" nvarchar({0}) COLLATE Modern_Spanish_CS_AI", (num > 0) ? num : 255);
                        break;
                    }
                }

                stringBuilder.Append(" NULL ,");
            }

            stringBuilder.Remove(stringBuilder.Length - 1, 1);
            stringBuilder.AppendFormat(" \n);\n");
            return SQLExec(stringBuilder.ToString());
        }

        public static bool _DbIndicesCampos(string tcNombreBd, ref DataTable tdtIndices)
        {
            return _DbIndicesCampos(tcNombreBd, ref tdtIndices, tlSoloClavesPrimarias: false);
        }

        public static bool _DbIndicesCampos(string tcNombreBd, ref DataTable tdtIndices, bool tlSoloClavesPrimarias)
        {
            if(string.IsNullOrWhiteSpace(tcNombreBd))
            {
                Error_Message = "No se ha recibido el nombre de la base de datos sobre la cual trabajar.";
                return false;
            }

            tcNombreBd = tcNombreBd.Trim();
            string text = (tlSoloClavesPrimarias ? " and ind.is_unique = 1" : "");
            return SQLExec(" \r\n                          SELECT t.name as tabla, ind.name as indice, ind.index_id as indice_id, ic.index_column_id as columna_id, \r\n                          col.name as columna, ind.type as indice_tipo_id, ind.type_desc as indice_tipo_desc, ind.is_unique as indice_unico, \r\n                          ind.is_primary_key as indice_primario, ic.key_ordinal as indice_orden, ic.is_descending_key as indice_primario_desc \r\n                          FROM [" + tcNombreBd + "].sys.indexes ind \r\n                          INNER JOIN [" + tcNombreBd + "].sys.index_columns ic ON  ind.object_id = ic.object_id and ind.index_id = ic.index_id \r\n                          INNER JOIN [" + tcNombreBd + "].sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id \r\n                          INNER JOIN [" + tcNombreBd + "].sys.tables t ON ind.object_id = t.object_id \r\n                          WHERE ind.type > 0 " + text + "\r\n                          ORDER BY t.name, ind.is_primary_key desc, ind.name, ind.index_id, ic.key_ordinal", ref tdtIndices);
        }

        public static bool _DbValoresDefectoCampos(string tcNombreBd, ref DataTable tdtValDefecto)
        {
            if(string.IsNullOrWhiteSpace(tcNombreBd))
            {
                Error_Message = "No se ha recibido el nombre de la base de datos sobre la cual trabajar.";
                return false;
            }

            tcNombreBd = tcNombreBd.Trim();
            return SQLExec("SELECT t.Name as tabla, c.Name as campo,  dc.Name as constr_nombre, dc.definition as constr_defin FROM [" + tcNombreBd + "].sys.tables t INNER JOIN [" + tcNombreBd + "].sys.default_constraints dc ON t.object_id = dc.parent_object_id INNER JOIN [" + tcNombreBd + "].sys.columns c ON dc.parent_object_id = c.object_id AND c.column_id = dc.parent_column_id ORDER BY t.Name", ref tdtValDefecto);
        }

        public static bool _DbTablasCampos(string tcNombreBd, ref DataTable tdtTablasCampos)
        {
            if(string.IsNullOrWhiteSpace(tcNombreBd))
            {
                Error_Message = "No se ha recibido el nombre de la base de datos sobre la cual trabajar.";
                return false;
            }

            tcNombreBd = tcNombreBd.Trim();
            return SQLExec("SELECT t.Name as tabla, c.Name as campo, c.is_nullable as permite_nulo FROM [" + tcNombreBd + "].sys.tables t INNER JOIN [" + tcNombreBd + "].sys.columns c ON t.object_id = c.object_id ORDER BY t.Name, c.name", ref tdtTablasCampos);
        }

        public static bool _Contiene_FuncionesDeAgregado(string[] taCampos)
        {
            for(int i = 0; i < taCampos.Length; i++)
            {
                if(_Contiene_FuncionesDeAgregado(taCampos[i]))
                {
                    return true;
                }
            }

            return false;
        }

        public static bool _SQLExisteBBDD(string tcNombreBBDD)
        {
            bool result = false;
            _ = string.Empty;
            DataTable dtTabla = new DataTable();
            if(string.IsNullOrWhiteSpace(tcNombreBBDD))
            {
                return result;
            }

            result = SQLExec("IF EXISTS (SELECT 1 FROM [master].sys.databases WHERE name = " + SQLString(tcNombreBBDD) + ")  SELECT 1 AS ExisteBBDD ELSE SELECT 0 AS ExisteBBDD ", ref dtTabla);
            if(result && dtTabla.Rows.Count > 0)
            {
                result = Convert.ToBoolean(dtTabla.Rows[0]["ExisteBBDD"]);
            }

            return result;
        }

        public static bool _GrupoEmpresa_Table2Xml(string tcArchivoGruposXml = "")
        {
            string text = "";
            bool result = false;
            if(!string.IsNullOrWhiteSpace(tcArchivoGruposXml))
            {
                text = tcArchivoGruposXml;
            }
            else
            {
                string text2 = Convert.ToString(_GetVariable("wc_iniservidor")).Trim();
                if(string.IsNullOrWhiteSpace(text2))
                {
                    return false;
                }

                text = System.IO.Path.Combine(text2, "gruposemp.xml");
            }

            DataTable dtTabla = new DataTable();
            SQLExec("select codigo,nombre,pripal,codpripal from " + SQLDatabase("EUROWINSYS", "GRUPOSEMP") + " order by codpripal,codigo", ref dtTabla);
            if(dtTabla != null && dtTabla.Rows.Count > 0)
            {
                if(File.Exists(text))
                {
                    File.Delete(text);
                }

                dtTabla.TableName = "grupo";
                dtTabla.WriteXml(text, XmlWriteMode.IgnoreSchema);
                if(File.Exists(text))
                {
                    result = true;
                }
            }

            return result;
        }

        public static bool _GrupoEmpresa_Xml2Table(string tcArchivoGruposXml = "")
        {
            string text = "";
            bool flag = false;
            if(!string.IsNullOrWhiteSpace(tcArchivoGruposXml))
            {
                text = tcArchivoGruposXml;
            }
            else
            {
                string text2 = Convert.ToString(_GetVariable("wc_iniservidor")).Trim();
                if(!string.IsNullOrWhiteSpace(text2))
                {
                    text = System.IO.Path.Combine(text2, "gruposemp.xml");
                }
            }

            if(string.IsNullOrWhiteSpace(text) || !File.Exists(text))
            {
                return false;
            }

            DataTable dataTable = new DataTable();
            DataSet dataSet = new DataSet();
            dataSet.ReadXml(text);
            if(dataSet.Tables.Count == 1)
            {
                dataTable = dataSet.Tables[0];
            }

            if(dataTable.Rows.Count > 0)
            {
                if(SQLExisteTabla("GRUPOSEMP"))
                {
                    SQLBegin();
                    flag = SQLExec("TRUNCATE TABLE " + SQLDatabase("EUROWINSYS", "GRUPOSEMP"));
                    if(flag)
                    {
                        SQLCommit();
                    }
                    else
                    {
                        SQLRollback();
                    }
                }
                else
                {
                    flag = _Crear_Tabla_GruposEmp();
                }

                if(flag)
                {
                    string text3 = "";
                    string text4 = "";
                    string text5 = "";
                    bool flag2 = false;
                    foreach(DataRow row in dataTable.Rows)
                    {
                        text3 = Convert.ToString(row["codigo"]);
                        text4 = Convert.ToString(row["nombre"]);
                        flag2 = Convert.ToBoolean(row["pripal"]);
                        text5 = Convert.ToString(row["codpripal"]);
                        if(!string.IsNullOrWhiteSpace(text3) && _SQLExisteBBDD("COMU" + text3))
                        {
                            flag = flag && SQLExec("INSERT INTO " + SQLDatabase("EUROWINSYS", "GRUPOSEMP") + " (CODIGO,NOMBRE,PRIPAL,CODPRIPAL) VALUES (" + SQLString(text3) + "," + SQLString(text4) + "," + SQLString(flag2) + "," + SQLString(text5) + ")");
                        }
                    }
                }
            }

            return flag;
        }

        public static bool _SQLExisteTablaBBDD(string tcNombreBBDD, string tcNombreTabla)
        {
            bool result = false;
            _ = string.Empty;
            DataTable dtTabla = new DataTable();
            if(string.IsNullOrWhiteSpace(tcNombreTabla))
            {
                return result;
            }

            if(string.IsNullOrWhiteSpace(tcNombreBBDD))
            {
                return result;
            }

            result = SQLExec("IF EXISTS (SELECT 1 FROM [master].sys.databases WHERE name = " + SQLString(tcNombreBBDD) + ")  SELECT 1 AS ExisteBBDD ELSE SELECT 0 AS ExisteBBDD ", ref dtTabla);
            if(result && dtTabla.Rows.Count > 0)
            {
                result = Convert.ToBoolean(dtTabla.Rows[0]["ExisteBBDD"]);
            }

            if(!result)
            {
                return false;
            }

            result = SQLExec("IF EXISTS (SELECT 1 FROM [" + tcNombreBBDD + "].INFORMATION_SCHEMA.TABLES WHERE table_name = " + SQLString(tcNombreTabla.Trim().ToLower()) + ")  SELECT 1 AS ExisteTabla ELSE SELECT 0 AS ExisteTabla ", ref dtTabla);
            if(result && dtTabla.Rows.Count > 0)
            {
                result = Convert.ToBoolean(dtTabla.Rows[0]["ExisteTabla"]);
            }

            return result;
        }

        public static string _Obtener_NuevaDb_Comunes(string tcPrefijo, int lnLenNombreBd)
        {
            Error_Message = "";
            if(!prefijoPresenteConLongitudCorrecta(tcPrefijo, lnLenNombreBd))
            {
                return "";
            }

            return NuevaBd(tcPrefijo, lnLenNombreBd);
        }

        public static string _Obtener_NuevaDb_2024XP(string tcPrefijo, int lnLenNombreBd, DataTable tdtBdsExcluir = null)
        {
            Error_Message = "";
            if(!prefijoPresenteConLongitudCorrecta(tcPrefijo, lnLenNombreBd))
            {
                return "";
            }

            DateTime today = DateTime.Today;
            int day = today.Day;
            int month = today.Month;
            int num = Convert.ToInt32(today.Year.ToString().Substring(2, 2));
            char c = (char)(65 + (day + month + num) % 25);
            return NuevaBd(tcPrefijo, lnLenNombreBd, Convert.ToString(c), tdtBdsExcluir);
        }

        private static bool prefijoPresenteConLongitudCorrecta(string tcPrefijo, int tnLenNombreBd)
        {
            if(string.IsNullOrWhiteSpace(tcPrefijo))
            {
                Error_Message = "No se ha recibido el parámetro prefijo para el nombre de la base de datos a obtener.";
                return false;
            }

            if(tnLenNombreBd == 0)
            {
                Error_Message = "No se ha recibido el parámetro longitud del nombre de la base de datos a obtener.";
                return false;
            }

            if(tnLenNombreBd <= tcPrefijo.Length)
            {
                Error_Message = "La longitud del parámetro prefijo para la obtención de la base de datos es superior a la longitud del nombre de la base de datos recibido como parámetro.";
                return false;
            }

            return true;
        }

        public static bool _Crear_Tabla_GruposEmp()
        {
            string empty = string.Empty;
            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[gruposemp](  [CODIGO] [char](4) NOT NULL,  [NOMBRE] [char](50) NOT NULL,  [PRIPAL] [bit] NOT NULL,  [CODPRIPAL] [char](4) NOT NULL,  [GUID_ID] [char](50) NOT NULL,  [CREATED] [datetime] NOT NULL,  [MODIFIED] [datetime] NOT NULL,  [CONTACT] [bit] NOT NULL,  [FREC_CTC] [int] NOT NULL,  [ULT_SYNC] [datetime] NULL,  [COPIA] [bit] NOT NULL,  [BCK_CFG] [text] NOT NULL,  [VISTA] [bit] NOT NULL,  [FECHA_CO] [bit] NOT NULL,  [AUTO_CON] [bit] NOT NULL,  [NOTIFICA] [bit] NOT NULL,  [LETRA_CAP] [char](2) NOT NULL,  [DES_FOTO] [bit] NOT NULL,  [FREC_DAS] [int] NOT NULL,  [EJER_CON] [char](15) NOT NULL,  CONSTRAINT [pk__eurowinsys__gruposemp__codigo] PRIMARY KEY CLUSTERED   (  [CODIGO] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind]  ) ON [eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__codigo]  DEFAULT ('') FOR [CODIGO]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__nombre]  DEFAULT ('') FOR [NOMBRE]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__pripal]  DEFAULT ((0)) FOR [PRIPAL]";
            bool num4 = num3 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__codpripal]  DEFAULT ('') FOR [CODPRIPAL]";
            bool num5 = num4 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__vista]  DEFAULT ((1)) FOR [VISTA]";
            bool num6 = num5 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__gruid_id]  DEFAULT (newid()) FOR [GUID_ID]";
            bool num7 = num6 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__created] DEFAULT (getdate()) FOR [CREATED]";
            bool num8 = num7 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__modified] DEFAULT (getdate()) FOR [MODIFIED]";
            bool num9 = num8 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__contact]  DEFAULT ((0)) FOR [CONTACT]";
            bool num10 = num9 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__frec_ctc]  DEFAULT ((0)) FOR [FREC_CTC]";
            bool num11 = num10 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__copia]  DEFAULT ((0)) FOR [COPIA]";
            bool num12 = num11 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__bck_cfg]  DEFAULT ('') FOR [BCK_CFG]";
            bool num13 = num12 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__fecha_co]  DEFAULT ((0)) FOR [FECHA_CO]";
            bool num14 = num13 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__auto_con]  DEFAULT ((0)) FOR [AUTO_CON]";
            bool num15 = num14 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__notifica]  DEFAULT ((0)) FOR [NOTIFICA]";
            bool num16 = num15 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__letra_cap]  DEFAULT ('') FOR [LETRA_CAP]";
            bool num17 = num16 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__des_foto]  DEFAULT ((0)) FOR [DES_FOTO]";
            bool num18 = num17 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__frec_das]  DEFAULT ((0)) FOR [FREC_DAS]";
            bool num19 = num18 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[gruposemp] ADD  CONSTRAINT [df__eurowinsys__gruposemp__ejer_con]  DEFAULT (('')) FOR [EJER_CON]";
            int num20;
            if(num19)
            {
                num20 = (SQLExec(empty) ? 1 : 0);
                if(num20 != 0)
                {
                    SQLCommit();
                    _DBsInformationSchema("EUROWINSYS")._AgregarTabla("GRUPOSEMP");
                    return (byte)num20 != 0;
                }
            }
            else
            {
                num20 = 0;
            }

            SQLRollback();
            return (byte)num20 != 0;
        }

        public static bool SQLConnect(string tcServer, string tcComunes = "")
        {
            return SQLConnect(tcServer, "", "", tcComunes, "master", 0, "sqlserver", "", 1800, tbAuthWin: true);
        }

        public static bool SQLConnect(string tcServer, string tcUser, string tcPassword, string tcComunes, string tcDatabase = "master", int tnPuerto = 0, string tcTipoServidor = "sqlserver", string tcAliasConexion = "", int tnConnectionTimout = 1800, bool tbAuthWin = false)
        {
            bool flag = false;
            string empty = string.Empty;
            string empty2 = string.Empty;
            empty2 = ((tnConnectionTimout <= 0) ? ("Persist Security Info=True;Server=" + tcServer + ";Database=" + tcDatabase) : ("Persist Security Info=True;Server=" + tcServer + ";Database=" + tcDatabase + ";Connection Timeout=" + tnConnectionTimout));
            if(tbAuthWin)
            {
                empty2 += ";Integrated Security = SSPI;";
                AuthWin = true;
            }
            else
            {
                empty2 = ((!string.IsNullOrEmpty(tcUser) && !string.IsNullOrEmpty(tcPassword)) ? (empty2 + ";Uid=" + tcUser + ";Pwd=" + tcPassword + ";") : (empty2 + ";Trusted_Connection=yes;"));
                UsuarioConex = tcUser;
                PasswordConex = tcPassword;
            }

            ServerConex = tcServer;
            if(!SQLComprobarConexion(empty2))
            {
                return false;
            }

            SqlConnection sqlConnection = new SqlConnection(empty2);
            try
            {
                sqlConnection.ConnectionString = empty2;
                _SQLOpen(sqlConnection);
                flag = sqlConnection.State == ConnectionState.Open;
                VersionSQLServer = sqlConnection.ServerVersion;
                sqlConnection.Close();
                if(!flag)
                {
                    return false;
                }

                Error_Message = string.Empty;
                Conexion = empty2;
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
                flag = false;
                return false;
            }

            if(!_ExternalConnect)
            {
                if(flag && !string.IsNullOrEmpty(tcComunes))
                {
                    DbComunes = tcComunes.Trim().ToUpper();
                    _Cargar_Alias_DB(tcComunes.ToLower().Trim());
                    _Carga_Modulos();
                    _Cargar_Alias_DB_Modulos();
                    _Carga_Modulos_Tablas();
                    _Carga_Registro_Consultas();
                }
                else
                {
                    _oAliasDB.Clear();
                }

                empty = tcAliasConexion;
                if(string.IsNullOrEmpty(empty))
                {
                    empty = tcComunes;
                    if(string.IsNullOrEmpty(empty))
                    {
                        empty = tcDatabase;
                    }
                }

                if(_oConexionDB.ContainsKey(empty))
                {
                    _oConexionDB.Remove(empty);
                }

                if(!_oConexionDB.ContainsKey(empty))
                {
                    Dictionary<string, object> dictionary = new Dictionary<string, object>();
                    SqlConnection sqlConnection2 = new SqlConnection();
                    sqlConnection2.ConnectionString = Conexion;
                    dictionary.Add("_oConexion", sqlConnection2);
                    Dictionary<string, string> value = new Dictionary<string, string>(_oAliasDB);
                    dictionary.Add("_oAliasDB", value);
                    _oConexionDB.Add(empty, dictionary);
                    CurrentAlias = empty;
                }

                Comprovacions_EurowinSys();
                DataTable dtTabla = new DataTable();
                SQLExec("SELECT CAST(SERVERPROPERTY('productversion') AS VARCHAR)+' - '+CAST(SERVERPROPERTY('productlevel') AS VARCHAR)+' ('+CAST(SERVERPROPERTY('edition') AS VARCHAR)+')' AS Versio", ref dtTabla);
                if(dtTabla != null && dtTabla.Rows.Count > 0)
                {
                    _cVersionMotor = dtTabla.Rows[0]["versio"].ToString().Trim();
                }
            }

            return flag;
        }

        public static bool SQLExistConnection(string tcServer, string tcUser, string tcPassword, string tcComunes, string tcDatabase = "master", int tnPuerto = 0, string tcTipoServidor = "sqlserver", int tnConnectionTimout = 1800, bool tbAuthWin = false)
        {
            bool result = false;
            _ = string.Empty;
            string empty = string.Empty;
            empty = ((tnConnectionTimout <= 0) ? ("Persist Security Info=True;Server=" + tcServer + ";Database=" + tcDatabase) : ("Persist Security Info=True;Server=" + tcServer + ";Database=" + tcDatabase + ";Connection Timeout=" + tnConnectionTimout));
            empty = (tbAuthWin ? (empty + ";Integrated Security = SSPI;") : ((!string.IsNullOrEmpty(tcUser) && !string.IsNullOrEmpty(tcPassword)) ? (empty + ";Uid=" + tcUser + ";Pwd=" + tcPassword + ";") : (empty + ";Trusted_Connection=yes;")));
            using(SqlConnection sqlConnection = new SqlConnection(empty))
            {
                try
                {
                    sqlConnection.ConnectionString = empty;
                    sqlConnection.Open();
                    result = sqlConnection.State == ConnectionState.Open;
                }
                catch(Exception)
                {
                    result = false;
                }
                finally
                {
                    sqlConnection.Close();
                }
            }

            return result;
        }

        public static bool SQLExec(string tcSql)
        {
            int tnFilasAfectadas;
            return SQLExec(tcSql, out tnFilasAfectadas);
        }

        public static bool SQLExecEjer(string tcSql, string[] tcEjercicios)
        {
            return sqlExecEjerPrv(tcSql, tcEjercicios);
        }

        private static bool sqlExecEjerPrv(string tcSql, string[] tcEjercicios, List<Tuple<string, string, SqlDbType>> parameters = null)
        {
            DataTable dtTabla = new DataTable();
            string text = "";
            string text2 = "";
            string text3 = "";
            string text4 = "";
            bool result = false;
            if(string.IsNullOrEmpty(Conexion))
            {
                Error_Message = "No se ha establecido la cadena de conexión a la bd.";
                return result;
            }

            if(string.IsNullOrEmpty(tcSql))
            {
                Error_Message = "No se ha indicado la consulta a realizar.";
                return result;
            }

            if(tcEjercicios.Length == 0 || tcEjercicios == null)
            {
                Error_Message = "No se han indicado los ejercicios.";
                return result;
            }

            text4 = " WHERE [ANY] IN (";
            for(int i = 0; i < tcEjercicios.Length; i++)
            {
                text4 += SQLString(tcEjercicios[i]);
                text4 = ((i == tcEjercicios.Length - 1) ? (text4 + ") ") : (text4 + ","));
            }

            text = "Select * From " + SQLDatabase("COMUNES", "EJERCICI") + " " + text4 + "Order By [any] Desc ";
            SQLExec(text, ref dtTabla);
            text = string.Empty;
            for(int j = 0; j < dtTabla.Rows.Count; j++)
            {
                text3 = dtTabla.Rows[j]["conexion"].ToString().ToLower().Trim();
                if(!string.IsNullOrEmpty(text3))
                {
                    text3 = "[" + text3 + "].dbo.";
                    text2 = tcSql.ToString().Replace("[multiples_ejercicios].dbo.", text3).Trim();
                    text += text2.ToString().Trim();
                    if(j != dtTabla.Rows.Count - 1)
                    {
                        text += " ; ";
                    }
                }
            }

            dtTabla.Dispose();
            int tnFilasAfectadas;
            return (parameters == null) ? SQLExec(text, out tnFilasAfectadas) : SQLExecParams(text, out tnFilasAfectadas, parameters);
        }

        private static void setModifiedInUpdate(ref string tcSql)
        {
            if(tcSql.IndexOf("MODIFIED", StringComparison.CurrentCultureIgnoreCase) < 0)
            {
                tcSql = Regex.Replace(tcSql, " SET ", " SET MODIFIED=GETDATE(), ", RegexOptions.IgnoreCase | RegexOptions.Compiled);
            }
        }

        public static bool SQLExec(string tcSql, out int tnFilasAfectadas)
        {
            SqlCommand sqlCommand = new SqlCommand();
            DateTime now = DateTime.Now;
            DateTime now2 = DateTime.Now;
            tnFilasAfectadas = 0;
            bool flag = _oTrans.ContainsKey(Thread.CurrentThread.ManagedThreadId);
            if(string.IsNullOrEmpty(Conexion))
            {
                Error_Message = "No se ha establecido la cadena de conexión a la bd.";
                return false;
            }

            if(string.IsNullOrEmpty(tcSql))
            {
                Error_Message = "No se ha indicado la instrucción sql a realizar.";
                return false;
            }

            if(tcSql.Trim().Substring(0, 3).ToUpper() == "UPD")
            {
                setModifiedInUpdate(ref tcSql);
            }

            if(Convert.ToBoolean(_GetVariable("_EdicionPerfiles")))
            {
                Registro_Log_Consultas = -1;
            }
            else
            {
                if(Registro_Log_Consultas == -1)
                {
                    Control_Modo_Log_Consultas();
                }

                if(Registro_Log_Consultas == 1)
                {
                    now = DateTime.Now;
                }
            }

            _QueryManager.ComprobarSiHayModificacionDeDatos(tcSql);
            bool tlOk = true;
            try
            {
                if(ReplicatedDB._SQLEXEC_Before != null)
                {
                    ReplicatedDB._SQLEXEC_Before(ref tcSql, ref tlOk);
                }
            }
            catch(Exception)
            {
                tlOk = true;
            }

            try
            {
                if(flag)
                {
                    Transac transac = _oTrans[Thread.CurrentThread.ManagedThreadId];
                    sqlCommand.Transaction = transac._oTransaccion;
                    sqlCommand.CommandText = tcSql;
                    sqlCommand.Connection = transac._oConexionTransaccion;
                    sqlCommand.CommandTimeout = 1800;
                    tnFilasAfectadas = sqlCommand.ExecuteNonQuery();
                }
                else
                {
                    SqlConnection sqlConnection = new SqlConnection(Conexion);
                    _SQLOpen(sqlConnection);
                    sqlCommand.CommandText = tcSql;
                    sqlCommand.Connection = sqlConnection;
                    sqlCommand.CommandTimeout = 1800;
                    tnFilasAfectadas = sqlCommand.ExecuteNonQuery();
                    if(!flag)
                    {
                        sqlConnection.Close();
                    }
                }

                if(Registro_Log_Consultas == 1)
                {
                    int value = Convert.ToInt32((DateTime.Now.Ticks - now.Ticks) / 10000);
                    Registrar_Sql(tcSql, Convert.ToInt32(value));
                }

                Registrar_Consultas(tcSql);
            }
            catch(Exception toEx)
            {
                Registrar_Sql(tnDuracionMiliSegundos: Convert.ToInt32(Convert.ToInt32((DateTime.Now.Ticks - now.Ticks) / 10000)), tcInstruccionSql: tcSql);
                Registrar_Consultas(tcSql);
                sqlCommand = null;
                Registrar_Error(toEx);
                return false;
            }

            if(_SQLEXEC_After != null)
            {
                DataTable tdtResult = null;
                _SQLEXEC_After(ref tdtResult);
            }
            //if(DB._SQLEXEC_After != null)
            //{
            //    DataTable tdtResult = null;
            //    DB._SQLEXEC_After(ref tdtResult);
            //}

            return true;
        }

        public static bool SQLExec(string tcSql, ref DataTable dtTabla, int tnNumeroRegistros = 0, bool tlNoResetDataTable = false)
        {
            new SqlDataAdapter();

            MessageBox.Show(
                "AT:"
                + "\n\n" +
                "ReplicatedDB.cs"
                + "\n\n" +
                "On:"
                + "\n\n" +
                "SQLExec(string tcSql, ref DataTable dtTabla, int tnNumeroRegistros = 0, bool tlNoResetDataTable = false)"
                + "\n\n" +
                "BEFORE:"
                + "\n\n" +
                "bool flag = _oTrans.ContainsKey(Thread.CurrentThread.ManagedThreadId);"
            );

            DateTime now = DateTime.Now;
            DateTime now2 = DateTime.Now;
            dtTabla.TableName = "mitabla";

            bool flag = _oTrans.ContainsKey(Thread.CurrentThread.ManagedThreadId);

            if(string.IsNullOrEmpty(Conexion))
            {
                Error_Message = "No se ha establecido la cadena de conexión a la bd.";
                return false;
            }

            if(string.IsNullOrEmpty(tcSql))
            {
                Error_Message = "No se ha indicado la consulta a realizar.";
                return false;
            }

            tcSql = tcSql.Trim();
            if(Convert.ToBoolean(_GetVariable("_EdicionPerfiles")))
            {
                Registro_Log_Consultas = -1;
            }
            else
            {
                if(Registro_Log_Consultas == -1)
                {
                    Control_Modo_Log_Consultas();
                }

                if(Registro_Log_Consultas == 1)
                {
                    now = DateTime.Now;
                }
            }

            if(!tlNoResetDataTable)
            {
                dtTabla.Reset();
            }

            if(tnNumeroRegistros > 0)
            {
                if(tcSql.IndexOf("order by", StringComparison.CurrentCultureIgnoreCase) <= 0)
                {
                    Error_Message = "Ha indicado un número de registros a devolver sin cláusula order by en la consulta a realizar.";
                    return false;
                }

                int num = tcSql.IndexOf("select", StringComparison.CurrentCultureIgnoreCase);
                tcSql = tcSql.Insert(num + 7, "top " + SQLString(tnNumeroRegistros) + " ");
            }

            bool tlOk = true;
            try
            {
                //if(DB._SQLEXEC_Before != null)
                //{
                //    DB._SQLEXEC_Before(ref tcSql, ref tlOk);
                //}
                if(_SQLEXEC_Before != null)
                {
                    _SQLEXEC_Before(ref tcSql, ref tlOk);
                }
            }
            catch(Exception)
            {
                tlOk = true;
            }

            if(!tlOk)
            {
                return false;
            }

            try
            {
                bool flag2 = false;
                if(flag)
                {
                    Transac transac = _oTrans[Thread.CurrentThread.ManagedThreadId];
                    SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(tcSql, transac._oConexionTransaccion);
                    sqlDataAdapter.SelectCommand.Transaction = transac._oTransaccion;
                    sqlDataAdapter.SelectCommand.CommandTimeout = 1800;
                    sqlDataAdapter.Fill(dtTabla);
                }
                else if(!_Persist)
                {
                    if(_UsamosCache())
                    {
                        flag2 = QueryCache.ContaninsKey(tcSql);
                        QueryEjecutable @object = new QueryEjecutable(tcSql);
                        dtTabla = QueryCache.GetObjectFromCache(tcSql, 2, @object.execQuery).DefaultView.ToTable();
                    }
                    else
                    {
                        SqlConnection sqlConnection = new SqlConnection(Conexion);
                        SqlDataAdapter obj = new SqlDataAdapter(tcSql, sqlConnection)
                    {
                            SelectCommand =
                        {
                            CommandTimeout = 1800
                        }
                        };
                        _SQLOpen(sqlConnection);
                        obj.Fill(dtTabla);
                        sqlConnection.Close();
                    }
                }
                else
                {
                    if(_oPersistConnection == null)
                    {
                        _oPersistConnection = new SqlConnection(Conexion);
                        _SQLOpen(_oPersistConnection);
                    }

                    SqlDataAdapter sqlDataAdapter2 = new SqlDataAdapter(tcSql, _oPersistConnection);
                    sqlDataAdapter2.SelectCommand.CommandTimeout = 1800;
                    sqlDataAdapter2.Fill(dtTabla);
                }

                if(Registro_Log_Consultas == 1)
                {
                    int value = Convert.ToInt32((DateTime.Now.Ticks - now.Ticks) / 10000);
                    Registrar_Sql(flag2 ? ("CACHE *** " + tcSql) : tcSql, Convert.ToInt32(value));
                }

                Registrar_Consultas(tcSql);
            }
            catch(Exception toEx)
            {
                if(Registro_Log_Consultas == 1)
                {
                    Registrar_Sql(tnDuracionMiliSegundos: Convert.ToInt32(Convert.ToInt32((DateTime.Now.Ticks - now.Ticks) / 10000)), tcInstruccionSql: tcSql);
                }

                Registrar_Consultas(tcSql);
                dtTabla = null;
                Registrar_Error(toEx);
                return false;
            }

            //if(DB._SQLEXEC_After != null)
            //{
            //    DB._SQLEXEC_After(ref dtTabla);
            //}
            if(_SQLEXEC_After != null)
            {
                _SQLEXEC_After(ref dtTabla);
            }

            return true;
        }

        public static List<ResultadoSQLExec> SQLExecParams(string tcSql, List<string> toGrupos, bool tlTodosEjercicios, List<Tuple<string, string, SqlDbType>> parameters)
        {
            return sqlExecPrv(tcSql, toGrupos, tlTodosEjercicios, null, parameters);
        }

        public static List<ResultadoSQLExec> SQLExecParams(string tcSql, List<string> toGrupos, List<Tuple<string, string, SqlDbType>> parameters, List<int> toEjercicios = null)
        {
            return sqlExecPrv(tcSql, toGrupos, tlTodosEjercicios: false, toEjercicios, parameters);
        }

        public static bool SQLExecEjerParams(string tcSql, ref DataTable dtTabla, DateTime tdFechaActual, DateTime tdFechaAnterior, List<Tuple<string, string, SqlDbType>> parameters)
        {
            return sqlExecEjerPrv(tcSql, ref dtTabla, tdFechaActual, tdFechaAnterior, parameters);
        }

        public static bool SQLExecEjerParams(string tcSql, ref DataTable dtTabla, int tnEjercicios, List<Tuple<string, string, SqlDbType>> parameters)
        {
            return sqlExecEjerPrv(tcSql, ref dtTabla, tnEjercicios, parameters);
        }

        public static bool SQLExecEjerParams(string tcSql, string[] tcEjercicios, List<Tuple<string, string, SqlDbType>> parameters)
        {
            return sqlExecEjerPrv(tcSql, tcEjercicios, parameters);
        }

        public static bool SQLExecEjerParams(string tcSql, ref DataTable dtTabla, string[] tcEjercicios, List<Tuple<string, string, SqlDbType>> parameters)
        {
            return sqlExecEjerPrv(tcSql, ref dtTabla, tcEjercicios, tlIncluirColEjer: true, parameters);
        }

        public static bool SQLExecEjer(string tcSql, ref DataTable dtTabla, string[] tcEjercicios, bool tlIncluirColEjer, List<Tuple<string, string, SqlDbType>> parameters)
        {
            return sqlExecEjerPrv(tcSql, ref dtTabla, tcEjercicios, tlIncluirColEjer, parameters);
        }

        public static bool SQLExecParams(string sql, ref DataTable tabla, List<Tuple<string, string, SqlDbType>> parameters, int tnNumeroRegistros = 0, bool tlNoResetDataTable = false)
        {
            SqlDataAdapter sqlDataAdapter = new SqlDataAdapter();
            DateTime now = DateTime.Now;
            DateTime now2 = DateTime.Now;
            tabla.TableName = "mitabla";
            bool flag = _oTrans.ContainsKey(Thread.CurrentThread.ManagedThreadId);
            if(string.IsNullOrEmpty(Conexion))
            {
                Error_Message = "No se ha establecido la cadena de conexión a la bd.";
                return false;
            }

            if(string.IsNullOrEmpty(sql))
            {
                Error_Message = "No se ha indicado la consulta a realizar.";
                return false;
            }

            sql = sql.Trim();
            if(Convert.ToBoolean(_GetVariable("_EdicionPerfiles")))
            {
                Registro_Log_Consultas = -1;
            }
            else
            {
                if(Registro_Log_Consultas == -1)
                {
                    Control_Modo_Log_Consultas();
                }

                if(Registro_Log_Consultas == 1)
                {
                    now = DateTime.Now;
                }
            }

            if(!tlNoResetDataTable)
            {
                tabla.Reset();
            }

            if(tnNumeroRegistros > 0)
            {
                if(sql.IndexOf("order by", StringComparison.CurrentCultureIgnoreCase) <= 0)
                {
                    Error_Message = "Ha indicado un número de registros a devolver sin cláusula order by en la consulta a realizar.";
                    return false;
                }

                int num = sql.IndexOf("select", StringComparison.CurrentCultureIgnoreCase);
                sql = sql.Insert(num + 7, "top " + SQLString(tnNumeroRegistros) + " ");
            }

            bool tlOk = true;
            try
            {
                if(ReplicatedDB._SQLEXEC_Before_Parameters != null)
                {
                    ReplicatedDB._SQLEXEC_Before_Parameters(ref sql, ref tlOk, ref parameters);
                }
            }
            catch(Exception)
            {
                tlOk = true;
            }

            if(!tlOk)
            {
                return false;
            }

            try
            {
                bool flag2 = false;
                if(flag)
                {
                    Transac transac = _oTrans[Thread.CurrentThread.ManagedThreadId];
                    sqlDataAdapter = new SqlDataAdapter(sql, transac._oConexionTransaccion);
                    sqlDataAdapter.SelectCommand.Transaction = transac._oTransaccion;
                    sqlDataAdapter.SelectCommand.CommandTimeout = 1800;
                    sqlDataAdapter.Fill(tabla);
                }
                else if(!_Persist)
                {
                    if(_UsamosCache())
                    {
                        flag2 = QueryCache.ContaninsKey(sql);
                        QueryEjecutable @object = new QueryEjecutable(sql);
                        tabla = QueryCache.GetObjectFromCache(sql, 2, @object.execQuery).DefaultView.ToTable();
                    }
                    else
                    {
                        SqlConnection sqlConnection = new SqlConnection(Conexion);
                        sqlDataAdapter = new SqlDataAdapter(sql, sqlConnection);
                        foreach(Tuple<string, string, SqlDbType> parameter in parameters)
                        {
                            sqlDataAdapter.SelectCommand.Parameters.Add(new SqlParameter {
                                ParameterName = parameter.Item1,
                                Value = parameter.Item2,
                                SqlDbType = parameter.Item3
                            });
                        }

                        sqlDataAdapter.SelectCommand.CommandTimeout = 1800;
                        _SQLOpen(sqlConnection);
                        sqlDataAdapter.Fill(tabla);
                        sqlConnection.Close();
                    }
                }
                else
                {
                    if(_oPersistConnection == null)
                    {
                        _oPersistConnection = new SqlConnection(Conexion);
                        _SQLOpen(_oPersistConnection);
                    }

                    sqlDataAdapter = new SqlDataAdapter(sql, _oPersistConnection);
                    sqlDataAdapter.SelectCommand.CommandTimeout = 1800;
                    sqlDataAdapter.Fill(tabla);
                }

                if(Registro_Log_Consultas == 1)
                {
                    int value = Convert.ToInt32((DateTime.Now.Ticks - now.Ticks) / 10000);
                    Registrar_Sql(flag2 ? ("CACHE *** " + sql) : sql, Convert.ToInt32(value));
                }

                Registrar_Consultas(sql);
            }
            catch(Exception toEx)
            {
                if(Registro_Log_Consultas == 1)
                {
                    Registrar_Sql(tnDuracionMiliSegundos: Convert.ToInt32(Convert.ToInt32((DateTime.Now.Ticks - now.Ticks) / 10000)), tcInstruccionSql: sql);
                }

                Registrar_Consultas(sql);
                sqlDataAdapter = null;
                tabla = null;
                Registrar_Error(toEx);
                return false;
            }

            if(ReplicatedDB._SQLEXEC_After != null)
            {
                ReplicatedDB._SQLEXEC_After(ref tabla);
            }

            return true;
        }

        public static bool SQLExecParams(string tcSql, List<Tuple<string, string, SqlDbType>> parameters)
        {
            int filasAfectadas;
            return SQLExecParams(tcSql, out filasAfectadas, parameters);
        }

        public static bool SQLExecParams(string sql, out int filasAfectadas, List<Tuple<string, string, SqlDbType>> parameters)
        {
            SqlCommand sqlCommand = new SqlCommand();
            DateTime now = DateTime.Now;
            DateTime now2 = DateTime.Now;
            filasAfectadas = 0;
            bool flag = _oTrans.ContainsKey(Thread.CurrentThread.ManagedThreadId);
            if(string.IsNullOrEmpty(Conexion))
            {
                Error_Message = "No se ha establecido la cadena de conexión a la bd.";
                return false;
            }

            if(string.IsNullOrEmpty(sql))
            {
                Error_Message = "No se ha indicado la instrucción sql a realizar.";
                return false;
            }

            if(sql.Trim().Substring(0, 3).ToUpper() == "UPD")
            {
                setModifiedInUpdate(ref sql);
            }

            if(Convert.ToBoolean(_GetVariable("_EdicionPerfiles")))
            {
                Registro_Log_Consultas = -1;
            }
            else
            {
                if(Registro_Log_Consultas == -1)
                {
                    Control_Modo_Log_Consultas();
                }

                if(Registro_Log_Consultas == 1)
                {
                    now = DateTime.Now;
                }
            }

            _QueryManager.ComprobarSiHayModificacionDeDatos(sql);
            bool tlOk = true;
            try
            {
                if(ReplicatedDB._SQLEXEC_Before_Parameters != null)
                {
                    ReplicatedDB._SQLEXEC_Before_Parameters(ref sql, ref tlOk, ref parameters);
                }
            }
            catch(Exception)
            {
                tlOk = true;
            }

            try
            {
                if(flag)
                {
                    Transac transac = _oTrans[Thread.CurrentThread.ManagedThreadId];
                    sqlCommand.Transaction = transac._oTransaccion;
                    sqlCommand.CommandText = sql;
                    sqlCommand.Connection = transac._oConexionTransaccion;
                    sqlCommand.CommandTimeout = 1800;
                    filasAfectadas = sqlCommand.ExecuteNonQuery();
                }
                else
                {
                    SqlConnection sqlConnection = new SqlConnection(Conexion);
                    foreach(Tuple<string, string, SqlDbType> parameter in parameters)
                    {
                        sqlCommand.Parameters.Add(new SqlParameter {
                            ParameterName = parameter.Item1,
                            Value = parameter.Item2,
                            SqlDbType = parameter.Item3
                        });
                    }

                    _SQLOpen(sqlConnection);
                    sqlCommand.CommandText = sql;
                    sqlCommand.Connection = sqlConnection;
                    sqlCommand.CommandTimeout = 1800;
                    filasAfectadas = sqlCommand.ExecuteNonQuery();
                    sqlConnection.Close();
                }

                if(Registro_Log_Consultas == 1)
                {
                    int value = Convert.ToInt32((DateTime.Now.Ticks - now.Ticks) / 10000);
                    Registrar_Sql(sql, Convert.ToInt32(value));
                }

                Registrar_Consultas(sql);
            }
            catch(Exception toEx)
            {
                Registrar_Sql(tnDuracionMiliSegundos: Convert.ToInt32(Convert.ToInt32((DateTime.Now.Ticks - now.Ticks) / 10000)), tcInstruccionSql: sql);
                Registrar_Consultas(sql);
                sqlCommand = null;
                Registrar_Error(toEx);
                return false;
            }

            if(ReplicatedDB._SQLEXEC_After != null)
            {
                DataTable tdtResult = null;
                ReplicatedDB._SQLEXEC_After(ref tdtResult);
            }

            return true;
        }

        public static List<InfoGrupo> _ObtenerInfoGrupos(List<string> toGrupos)
        {
            StringBuilder stringBuilder = new StringBuilder();
            DataTable dtTabla = new DataTable();
            new List<InfoGrupo>();
            List<string> list = toGrupos.Where((string f) => !_oGrupos.ContainsKey(f)).ToList();
            if(_oGrupos.Count == 0 || list.Count() > 0)
            {
                foreach(string item in list)
                {
                    if(_DataBases.Contains($"COMU{item}"))
                    {
                        if(stringBuilder.Length > 0)
                        {
                            stringBuilder.Append(" UNION ");
                        }

                        stringBuilder.AppendFormat("SELECT '{0}' as CODIGO, '' as [ANY],  '' as CONEXION , m.NOMBRE, m.NOM_CONEX, 0 as PREDET FROM [COMU{0}].dbo.modulos m UNION SELECT '{0}' as CODIGO, e.[ANY], e.CONEXION , '' as NOMBRE, '' as NOM_CONEX, e.PREDET FROM [COMU{0}].dbo.EJERCICI e ", item);
                    }
                }

                if(SQLExec(stringBuilder.ToString(), ref dtTabla))
                {
                    (from loRow in dtTabla.AsEnumerable()
                     group loRow by new
                     {
                         Codigo = Convert.ToString(loRow["CODIGO"])
                     } into g
                     select new InfoGrupo {
                         Codigo = g.Key.Codigo,
                         Grupo = $"COMU{g.Key.Codigo}",
                         Ejercicios = (from loRow in g
                                       where !string.IsNullOrEmpty(Convert.ToString(loRow["CONEXION"]))
                                       group loRow by new
                                       {
                                           Ejercicio = Convert.ToString(loRow["ANY"]).Trim(),
                                           Conexion = Convert.ToString(loRow["CONEXION"]).Trim()
                                       } into gr
                                       select new
                                       {
                                           gr.Key.Ejercicio,
                                           gr.Key.Conexion
                                       }).ToList().ToDictionary(a => a.Ejercicio, a => a.Conexion),
                         EjercicioPredet = (from loRow in g
                                            where !string.IsNullOrEmpty(Convert.ToString(loRow["CONEXION"])) && Convert.ToInt16(loRow["PREDET"]) != 0
                                            group loRow by new
                                            {
                                                Ejercicio = Convert.ToString(loRow["ANY"]).Trim(),
                                                Conexion = Convert.ToString(loRow["CONEXION"]).Trim()
                                            } into gr
                                            select new { gr.Key.Ejercicio }).FirstOrDefault().Ejercicio.ToString(),
                         Addons = (from loRow in g
                                   where !string.IsNullOrEmpty(Convert.ToString(loRow["NOMBRE"]))
                                   group loRow by new
                                   {
                                       Nombre = Convert.ToString(loRow["NOMBRE"]).Trim(),
                                       NombreConexion = Convert.ToString(loRow["NOM_CONEX"]).Trim()
                                   } into gr
                                   select new
                                   {
                                       gr.Key.Nombre,
                                       gr.Key.NombreConexion
                                   }).ToList().ToDictionary(a => a.Nombre, a => a.NombreConexion)
                     }).ToList().ForEach(delegate (InfoGrupo f) {
                         _oGrupos.Add(f.Codigo, f);
                     });
                }
            }

            return _oGrupos.Where(delegate (KeyValuePair<string, InfoGrupo> loGrupo) {
                List<string> list2 = toGrupos;
                KeyValuePair<string, InfoGrupo> keyValuePair2 = loGrupo;
                return list2.Contains(keyValuePair2.Key);
            }).Select(delegate (KeyValuePair<string, InfoGrupo> loGrupo) {
                KeyValuePair<string, InfoGrupo> keyValuePair = loGrupo;
                return keyValuePair.Value;
            }).ToList();
        }

        public static List<ResultadoSQLExec> SQLExec(string tcSql, List<string> toGrupos, bool tlTodosEjercicios)
        {
            return sqlExecPrv(tcSql, toGrupos, tlTodosEjercicios, new List<int> { Convert.ToInt32(Ejercicio_EW) });
        }

        public static List<ResultadoSQLExec> SQLExec(string tcSql, List<string> toGrupos, List<int> toEjercicios = null)
        {
            return sqlExecPrv(tcSql, toGrupos, tlTodosEjercicios: false, toEjercicios);
        }

        private static List<ResultadoSQLExec> sqlExecPrv(string tcSql, List<string> toGrupos, bool tlTodosEjercicios, List<int> toEjercicios = null, List<Tuple<string, string, SqlDbType>> parameters = null)
        {
            new List<string>();
            List<ResultadoSQLExec> list = new List<ResultadoSQLExec>();
            if(!string.IsNullOrEmpty(tcSql))
            {
                List<ResultadoTransformacionSQL> list2 = _PrepareSQl(tcSql, toGrupos, tlTodosEjercicios, toEjercicios);
                if(list2.Count > 0)
                {
                    foreach(ResultadoTransformacionSQL item2 in list2)
                    {
                        ResultadoSQLExec item = ((!item2.Resultado) ? new ResultadoSQLExec(item2) : _ExecuteSQL(item2.Grupo, item2.Ejercicio, item2.Consulta, parameters));
                        list.Add(item);
                    }
                }
                else
                {
                    ResultadoSQLExec item = _ExecuteSQL(DbComunes, Ejercicio_EW, tcSql);
                    list.Add(item);
                }
            }

            return list;
        }

        private static List<ResultadoTransformacionSQL> _PrepareSQl(string tcSql, List<string> toGrupos, bool tlTodosEjercicios, List<int> toEjercicios)
        {
            List<string> list = new List<string>();
            List<string> list2 = new List<string>();
            List<string> list3 = new List<string>();
            List<ResultadoTransformacionSQL> list4 = new List<ResultadoTransformacionSQL>();
            List<InfoGrupo> list5 = _ObtenerInfoGrupos(toGrupos);
            tcSql = tcSql.Replace("!=", "<>");
            MatchCollection matchCollection = Regex.Matches(tcSql, "\\s(?<DATABASE>\\w*!)\\w*");
            if(matchCollection.Count > 0)
            {
                list = (from Match loMatch in matchCollection
                        where Convert.ToString(loMatch.Groups["DATABASE"]).ToUpper() == "2024XP!"
                        select Convert.ToString(loMatch.Groups["DATABASE"]).ToUpper()).Distinct().ToList();
                list2 = (from Match loMatch in matchCollection
                         where Convert.ToString(loMatch.Groups["DATABASE"]).ToUpper() == "COMUNES!"
                         select Convert.ToString(loMatch.Groups["DATABASE"]).ToUpper()).Distinct().ToList();
                list3 = (from Match loMatch in matchCollection
                         where Convert.ToString(loMatch.Groups["DATABASE"]).ToUpper() != "2024XP!" && Convert.ToString(loMatch.Groups["DATABASE"]).ToUpper() != "COMUNES!"
                         select Convert.ToString(loMatch.Groups["DATABASE"]).ToUpper()).Distinct().ToList();
            }

            matchCollection = Regex.Matches(tcSql, "\\s(?<DATABASE>[0-9]{4}[a-zA-Z]{2}.dbo.)|\\s(?<DATABASE>\\[[0-9]{4}[a-zA-Z]{2}\\].dbo.)");
            if(matchCollection.Count > 0)
            {
                foreach(Match item2 in matchCollection)
                {
                    list.Add(Convert.ToString(item2.Groups["DATABASE"]));
                }
            }

            matchCollection = Regex.Matches(tcSql, "\\s(?<DATABASE>COMU[[0-9]{4}.dbo.)|\\s(?<DATABASE>\\[COMU[[0-9]{4}\\].dbo.)", RegexOptions.IgnoreCase);
            if(matchCollection.Count > 0)
            {
                foreach(Match item3 in matchCollection)
                {
                    list2.Add(Convert.ToString(item3.Groups["DATABASE"]));
                }
            }

            matchCollection = Regex.Matches(tcSql, "\\s(?<DATABASE>[0-9a-zA-Z]{8}.dbo.)|\\s(?<DATABASE>\\[[0-9a-zA-Z]{8}\\].dbo.)");
            if(matchCollection.Count > 0)
            {
                foreach(Match item4 in matchCollection)
                {
                    string item = Convert.ToString(item4.Groups["DATABASE"]);
                    if(!list2.Contains(item))
                    {
                        list3.Add(item);
                    }
                }
            }

            if(list2.Count > 0 || list3.Count > 0 || list.Count > 0)
            {
                foreach(InfoGrupo item5 in list5)
                {
                    try
                    {
                        string text = tcSql;
                        if(list2.Count > 0 || list3.Count > 0)
                        {
                            foreach(string item6 in list2.Distinct())
                            {
                                text = Regex.Replace(text, Regex.Escape(item6), item5.SQLDataBase("COMUNES"), RegexOptions.IgnoreCase);
                            }

                            foreach(string item7 in list3.Distinct())
                            {
                                string lcAddonTmp = Regex.Replace(item7, ".dbo.|[^0-9a-zA-Z]|", "", RegexOptions.IgnoreCase).ToUpper();
                                if(item5.Addons.ContainsKey(lcAddonTmp) || (lcAddonTmp.Length > 5 && item5.Addons.Keys.Where((string f) => f.StartsWith(lcAddonTmp.Substring(0, 6))).Count() > 0))
                                {
                                    text = Regex.Replace(text, Regex.Escape(item7), item5.SQLDataBase(lcAddonTmp), RegexOptions.IgnoreCase);
                                    continue;
                                }

                                throw new Exception($"El addon no existe para el grupo {item5.Grupo}");
                            }
                        }

                        if(list.Count > 0)
                        {
                            List<string> list6 = item5.Ejercicios.Keys.Where((string loEjer) => tlTodosEjercicios || loEjer == Ejercicio_EW).ToList();
                            if(tlTodosEjercicios)
                            {
                                list6 = item5.Ejercicios.Keys.ToList();
                            }
                            else if(toEjercicios != null && toEjercicios.Count > 0)
                            {
                                list6 = item5.Ejercicios.Keys.Where((string f) => toEjercicios.Contains(Convert.ToInt32(f))).ToList();
                            }
                            else if(toEjercicios == null)
                            {
                                list6 = new List<string> { item5.EjercicioPredet };
                            }

                            foreach(string item8 in list.Distinct())
                            {
                                foreach(string item9 in list6)
                                {
                                    string consulta = Regex.Replace(text, Regex.Escape(item8), item5.SQLDataBase("2024XP", item9), RegexOptions.IgnoreCase);
                                    list4.Add(new ResultadoTransformacionSQL {
                                        Grupo = item5.Grupo,
                                        Ejercicio = item9,
                                        Consulta = consulta,
                                        Error = "",
                                        Resultado = true
                                    });
                                }
                            }
                        }
                        else
                        {
                            list4.Add(new ResultadoTransformacionSQL {
                                Grupo = item5.Grupo,
                                Ejercicio = Ejercicio_EW,
                                Consulta = text,
                                Error = "",
                                Resultado = true
                            });
                        }
                    }
                    catch(Exception ex)
                    {
                        list4.Add(new ResultadoTransformacionSQL {
                            Grupo = item5.Grupo,
                            Ejercicio = Ejercicio_EW,
                            Consulta = tcSql,
                            Error = ex.Message,
                            Resultado = false
                        });
                    }
                }
            }

            return list4;
        }

        private static ResultadoSQLExec _ExecuteSQL(string tcGrupo, string tcEjercicio, string tcSql, List<Tuple<string, string, SqlDbType>> parameters = null)
        {
            DataTable tabla = new DataTable();
            ResultadoSQLExec resultadoSQLExec = new ResultadoSQLExec
        {
                Grupo = tcGrupo,
                Ejercicio = tcEjercicio,
                Consulta = tcSql
            };
            if(resultadoSQLExec.Resultado = ((parameters == null) ? SQLExec(tcSql, ref tabla) : SQLExecParams(tcSql, ref tabla, parameters)))
            {
                resultadoSQLExec.Resultados = tabla;
                resultadoSQLExec.Error = string.Empty;
            }
            else
            {
                resultadoSQLExec.Error = Error_Message;
            }

            return resultadoSQLExec;
        }

        public static void _SetTimeCacheTo(int tnMinutos)
        {
            _ = _nTimeCache;
            _nTimeCache = tnMinutos;
            if(_nTimeCache == 0)
            {
                //////////////////////////////////////
                //////////////////////////////////////
                //////////////////////////////////////
                //////////////////////////////////////
                //////////////////////////////////////
                //////////////////////////////////////
                //////////////////////////////////////
                //////////////////////////////////////
                //////////////////////////////////////
                //QueryCache.VaciarCache();
                _QueryManager.RestablecerTipoConsultaAnterior();
            }
            else
            {
                _QueryManager.CambiarTipoConsultasA(eTipoQuery.Cache);
            }
        }

        public static bool _UsamosCache()
        {
            return _nTimeCache > 0;
        }

        public static bool SQLExecEjer(string tcSql, ref DataTable dtTabla, DateTime tdFechaActual, DateTime tdFechaAnterior)
        {
            return sqlExecEjerPrv(tcSql, ref dtTabla, tdFechaActual, tdFechaAnterior);
        }

        private static bool sqlExecEjerPrv(string tcSql, ref DataTable dtTabla, DateTime tdFechaActual, DateTime tdFechaAnterior, List<Tuple<string, string, SqlDbType>> parameters = null)
        {
            DataTable dtTabla2 = new DataTable();
            dtTabla.TableName = "mitabla";
            int num = 0;
            string text = "";
            string text2 = "";
            bool result = false;
            if(string.IsNullOrEmpty(Conexion))
            {
                Error_Message = "No se ha establecido la cadena de conexión a la bd.";
                return result;
            }

            if(string.IsNullOrEmpty(tcSql))
            {
                Error_Message = "No se ha indicado la consulta a realizar.";
                return result;
            }

            text = "Select * From " + SQLDatabase("COMUNES", "EJERCICI") + " Where PeriodoIni >= " + SQLString(tdFechaAnterior) + " And PeriodoFin <= " + SQLString(tdFechaActual) + " Order By [any] Desc ";
            SQLExec(text, ref dtTabla2);
            foreach(DataRow row in dtTabla2.Rows)
            {
                text2 = row["conexion"].ToString().ToLower().Trim();
                if(string.IsNullOrEmpty(text2))
                {
                    continue;
                }

                text2 = "[" + text2 + "].dbo.";
                text = tcSql.ToString().Replace("[multiples_ejercicios].dbo.", text2).Trim();
                if(num == 0)
                {
                    if((parameters == null) ? SQLExec(text, ref dtTabla) : SQLExecParams(text, ref dtTabla, parameters))
                    {
                        num++;
                        result = true;
                    }

                    continue;
                }

                DataTable dtTabla3 = new DataTable();
                if((parameters == null) ? SQLExec(text, ref dtTabla3) : SQLExecParams(text, ref dtTabla3, parameters))
                {
                    if(dtTabla3.Rows.Count > 0)
                    {
                        DBfunctions.SQLUnionDatatable(ref dtTabla, dtTabla3);
                    }

                    dtTabla3.Dispose();
                }
            }

            dtTabla2.Dispose();
            return result;
        }

        public static bool SQLExecEjer(string tcSql, ref DataTable dtTabla, int tnEjercicios)
        {
            return sqlExecEjerPrv(tcSql, ref dtTabla, tnEjercicios);
        }

        private static bool sqlExecEjerPrv(string tcSql, ref DataTable dtTabla, int tnEjercicios, List<Tuple<string, string, SqlDbType>> parameters = null)
        {
            DataTable dtTabla2 = new DataTable();
            dtTabla.TableName = "mitabla";
            int num = 0;
            string text = "";
            string text2 = "";
            bool result = false;
            if(string.IsNullOrEmpty(Conexion))
            {
                Error_Message = "No se ha establecido la cadena de conexión a la bd.";
                return result;
            }

            if(string.IsNullOrEmpty(tcSql))
            {
                Error_Message = "No se ha indicado la consulta a realizar.";
                return result;
            }

            text = "Select * From " + SQLDatabase("COMUNES", "EJERCICI") + " Order By [any] Desc ";
            SQLExec(text, ref dtTabla2, tnEjercicios + 1);
            foreach(DataRow row in dtTabla2.Rows)
            {
                text2 = row["conexion"].ToString().ToLower().Trim();
                if(string.IsNullOrEmpty(text2))
                {
                    continue;
                }

                text2 = "[" + text2 + "].dbo.";
                text = tcSql.ToString().Replace("[multiples_ejercicios].dbo.", text2).Trim();
                if(num == 0)
                {
                    if((parameters == null) ? SQLExec(text, ref dtTabla) : SQLExecParams(text, ref dtTabla, parameters))
                    {
                        num++;
                        result = true;
                    }

                    continue;
                }

                DataTable dtTabla3 = new DataTable();
                if((parameters == null) ? SQLExec(text, ref dtTabla3) : SQLExecParams(text, ref dtTabla3, parameters))
                {
                    if(dtTabla3.Rows.Count > 0)
                    {
                        DBfunctions.SQLUnionDatatable(ref dtTabla, dtTabla3);
                    }

                    dtTabla3.Dispose();
                }
            }

            dtTabla2.Dispose();
            return result;
        }

        public static bool SQLExecEjer(string tcSql, ref DataTable dtTabla, string[] tcEjercicios)
        {
            return sqlExecEjerPrv(tcSql, ref dtTabla, tcEjercicios, tlIncluirColEjer: true);
        }

        public static bool SQLExecEjer(string tcSql, ref DataTable dtTabla, string[] tcEjercicios, bool tlIncluirColEjer)
        {
            return sqlExecEjerPrv(tcSql, ref dtTabla, tcEjercicios, tlIncluirColEjer);
        }

        private static bool sqlExecEjerPrv(string tcSql, ref DataTable dtTabla, string[] tcEjercicios, bool tlIncluirColEjer, List<Tuple<string, string, SqlDbType>> parameters = null)
        {
            dtTabla.TableName = "mitabla";
            string text = "";
            string text2 = "";
            string text3 = "";
            string text4 = "";
            string empty = string.Empty;
            bool result = false;
            if(string.IsNullOrEmpty(Conexion))
            {
                Error_Message = "No se ha establecido la cadena de conexión a la bd.";
                return result;
            }

            if(string.IsNullOrEmpty(tcSql))
            {
                Error_Message = "No se ha indicado la consulta a realizar.";
                return result;
            }

            if(tcEjercicios == null || tcEjercicios.Length == 0)
            {
                tcEjercicios = new string[1] { Ejercicio_EW };
            }

            if(tcEjercicios == null || tcEjercicios.Length == 0)
            {
                Error_Message = "No se han indicado los ejercicios.";
                return result;
            }

            empty = " WHERE [ANY] IN (";
            for(int i = 0; i < tcEjercicios.Length; i++)
            {
                empty += SQLString(tcEjercicios[i]);
                empty = ((i == tcEjercicios.Length - 1) ? (empty + ") ") : (empty + ","));
            }

            int num = tcSql.LastIndexOf("ORDER BY", StringComparison.InvariantCultureIgnoreCase);
            string text5;
            string text6;
            if(num != -1)
            {
                text5 = tcSql.Substring(0, num);
                text6 = tcSql.Substring(num);
            }
            else
            {
                text5 = tcSql;
                text6 = "";
            }

            tcSql = text5;
            foreach(KeyValuePair<string, string> item in _oAliasDBEjer)
            {
                text3 = item.Value;
                text4 = item.Key;
                if(string.IsNullOrEmpty(text3) || !tcEjercicios.Contains(text4))
                {
                    continue;
                }

                if(!string.IsNullOrWhiteSpace(text) && tcEjercicios.Count() > 1)
                {
                    text = text + Environment.NewLine + Environment.NewLine + " UNION ALL " + Environment.NewLine + Environment.NewLine;
                }

                text2 = tcSql.ToString().Replace("[multiples_ejercicios].dbo.", text3).Trim();
                if(tlIncluirColEjer)
                {
                    if(text2.ToUpper().Contains("DISTINCT"))
                    {
                        text2 = new Regex("DISTINCT ", RegexOptions.IgnoreCase).Replace(text2, " ");
                        text2 = new Regex("SELECT ", RegexOptions.IgnoreCase).Replace(text2, "SELECT DISTINCT '" + text4 + "' AS EJERCICIO,").Trim();
                    }
                    else
                    {
                        text2 = text2.ToString().Replace("SELECT ", "SELECT '" + text4 + "' AS EJERCICIO,").Trim();
                    }
                }

                text2 = text2.ToString().Replace("@EJERCICIO", text4).Trim();
                text2 = text2.ToString().Replace("@SEL", "SELECT").Trim();
                text += text2;
            }

            text = text + " " + text6;
            if(parameters != null)
            {
                return SQLExecParams(text, ref dtTabla, parameters);
            }

            return SQLExec(text, ref dtTabla);
        }

        public static bool SQLPivot(string tcColumnasPivot, string tcSqlSourcetable, string tcAgregadoPivot, string tcInPivot, ref DataTable dtResultado)
        {
            _ = string.Empty;
            string lcConexion = string.Empty;
            string empty = string.Empty;
            _oAliasDB.TryGetValue("2024XP", out lcConexion);
            empty = _oAliasDB.FirstOrDefault((KeyValuePair<string, string> x) => x.Value == lcConexion && x.Key != "2024XP").Key;
            tcSqlSourcetable = tcSqlSourcetable.ToString().Replace("SELECT ", "SELECT '" + empty + "' AS [EJERCICIO],").Trim();
            return SQLExec("SELECT " + tcColumnasPivot + " FROM (" + tcSqlSourcetable + ") AS SourceTable PIVOT (" + tcAgregadoPivot + " FOR " + tcInPivot + ") AS PivotTable", ref dtResultado);
        }

        public static bool SQLPivotEjer(SqlPivotConfig toPivotConfig, ref DataTable dtResultado)
        {
            DataTable dtTabla = new DataTable();
            dtResultado.TableName = "mitabla";
            string text = "";
            string text2 = "";
            string text3 = "";
            string text4 = "";
            string text5 = "";
            string empty = string.Empty;
            bool result = false;
            if(string.IsNullOrEmpty(Conexion))
            {
                Error_Message = "No se ha establecido la cadena de conexión a la bd.";
                return result;
            }

            if(!toPivotConfig.IsValid())
            {
                Error_Message = toPivotConfig.Error_Message;
                return result;
            }

            empty = " WHERE [ANY] IN (";
            string text6 = "";
            foreach(string key in toPivotConfig.EjerciciosConSusFiltrosEspecificos.Keys)
            {
                empty = empty + text6 + SQLString(key);
                text6 = ", ";
            }

            empty += ") ";
            text = "Select * From " + SQLDatabase("COMUNES", "EJERCICI") + " " + empty + "Order By [any] Desc ";
            SQLExec(text, ref dtTabla);
            text = string.Empty;
            for(int i = 0; i < dtTabla.Rows.Count; i++)
            {
                text4 = dtTabla.Rows[i]["conexion"].ToString().ToLower().Trim();
                text5 = dtTabla.Rows[i]["any"].ToString().ToLower().Trim();
                if(!string.IsNullOrEmpty(text4))
                {
                    text4 = "[" + text4 + "].dbo.";
                    text2 = toPivotConfig.SqlSourcetable.Replace("[multiples_ejercicios].dbo.", text4).Trim();
                    text3 += text2.ToString().Replace("SELECT ", "SELECT '" + text5 + "' AS [EJERCICIO],").Trim();
                    if(toPivotConfig.EjerciciosConSusFiltrosEspecificos.ContainsKey(text5))
                    {
                        text3 = text3 + " " + toPivotConfig.EjerciciosConSusFiltrosEspecificos[text5] + " ";
                    }

                    text3 = text3 + " " + toPivotConfig.SqlSourcetableGroupBy + " ";
                    text3 += toPivotConfig.QueryGeneracionFilaFicticia.Replace("SELECT ", "SELECT '" + text5 + "' AS [EJERCICIO],").Trim();
                    if(i != dtTabla.Rows.Count - 1)
                    {
                        text3 += " UNION ALL ";
                    }
                }
            }

            dtTabla.Dispose();
            text = "SELECT " + toPivotConfig.ColumnasPivot + " FROM (" + text3 + ") AS SourceTable PIVOT (" + toPivotConfig.AgregadoPivot + " FOR " + toPivotConfig.InPivot + ") AS PivotTable";
            if(!string.IsNullOrWhiteSpace(toPivotConfig.OrderBy))
            {
                text += toPivotConfig.OrderBy;
            }

            return SQLExec(text, ref dtResultado);
        }

        public static bool SQLPivotEjer(string tcColumnasPivot, string tcSqlSourcetable, string tcAgregadoPivot, string tcInPivot, ref DataTable dtResultado, string tcFilaFicticia, Dictionary<string, string> tdicEjerciciosFiltros, string tcOrderBy = "")
        {
            return SQLPivotEjer(new SqlPivotConfig {
                ColumnasPivot = tcColumnasPivot,
                SqlSourcetable = tcSqlSourcetable,
                AgregadoPivot = tcAgregadoPivot,
                InPivot = tcInPivot,
                QueryGeneracionFilaFicticia = tcFilaFicticia,
                EjerciciosConSusFiltrosEspecificos = tdicEjerciciosFiltros,
                OrderBy = tcOrderBy
            }, ref dtResultado);
        }

        public static string SQLString(object txValor, int tnLongitud = 0, char tcRelleno = ' ', bool tlIzquierda = false)
        {
            string result = string.Empty;
            if(txValor == null || DBNull.Value.Equals(txValor))
            {
                return "null";
            }

            try
            {
                switch(txValor.GetType().ToString().Trim()
                    .ToLower())
                {
                    case "system.boolean":
                        result = ((!(bool)txValor) ? "0" : "1");
                        break;
                    case "system.datetime":
                    {
                        DateTime dateTime = (DateTime)txValor;
                        result = "'" + dateTime.Day.ToString().Trim() + "/" + dateTime.Month.ToString().Trim() + "/" + dateTime.Year.ToString().Trim();
                        if(dateTime.Hour != 0 || dateTime.Minute != 0 || dateTime.Second != 0)
                        {
                            result = result + " " + dateTime.Hour.ToString().Trim() + ":" + dateTime.Minute.ToString().Trim() + ":" + dateTime.Second.ToString().Trim();
                        }

                        result += "'";
                        break;
                    }
                    case "system.decimal":
                    case "system.double":
                    case "system.int16":
                    case "system.int32":
                    case "system.int64":
                    case "system.uint16":
                    case "system.uint32":
                    case "system.uint64":
                        result = txValor.ToString().Replace(",", ".").Trim();
                        break;
                    case "system.string":
                        result = txValor.ToString();
                        if(tnLongitud > 0)
                        {
                            result = result.Trim();
                            result = ((!tlIzquierda) ? result.PadRight(tnLongitud, tcRelleno) : result.PadLeft(tnLongitud, tcRelleno));
                        }

                        result = "'" + result.Replace("'", "''") + "'";
                        break;
                }
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
                return string.Empty;
            }

            return result;
        }

        public static string SQLDatabaseReal(string tcDatabaseReal, string tcTabla)
        {
            if(string.IsNullOrWhiteSpace(tcDatabaseReal) && string.IsNullOrWhiteSpace(tcTabla))
            {
                return string.Empty;
            }

            return "[" + tcDatabaseReal.ToUpper().Trim() + "].dbo.[" + tcTabla.ToUpper().Trim() + "]";
        }

        public static string SQLDatabase(string tcDatabase, string tcTabla)
        {
            return SQLDatabase(tcDatabase, tcTabla, tlSys: false);
        }

        public static string SQLDatabase(string tcDatabase, string tcTabla, bool tlSys = false)
        {
            string empty = string.Empty;
            if(string.IsNullOrWhiteSpace(tcDatabase) && string.IsNullOrWhiteSpace(tcTabla))
            {
                return string.Empty;
            }

            tcDatabase = tcDatabase.ToUpper().Trim();
            try
            {
                if(string.IsNullOrWhiteSpace(tcDatabase))
                {
                    empty = "[multiples_ejercicios].dbo." + tcTabla.Trim();
                }
                else
                {
                    empty = ParseDatabase(tcDatabase, tcTabla);
                    if(tlSys)
                    {
                        empty = empty.Replace(".dbo.", ".sys.");
                    }
                }
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
                return string.Empty;
            }

            return empty;
        }

        private static string ParseDatabase(string tcDatabase, string tcTabla)
        {
            return ParseDatabase(tcDatabase).Trim() + tcTabla.Trim();
        }

        private static string ParseDatabase(string tcDatabase)
        {
            tcDatabase = tcDatabase.ToUpper().Trim();
            if(tcDatabase.Contains("2024XP") && tcDatabase.Contains('-') && tcDatabase.Length > tcDatabase.IndexOf('-') + 1)
            {
                int result = 0;
                if(!int.TryParse(tcDatabase.Substring(tcDatabase.IndexOf('-') + 1), out result))
                {
                    throw new Exception($"'{tcDatabase.Substring(tcDatabase.IndexOf('-') + 1)}' no es un valor númerico. Y no se puede tratar.");
                }

                int num = Convert.ToInt32(Ejercicio_EW) - result;
                if(!_oAliasDB.ContainsKey(Convert.ToString(num)))
                {
                    throw new Exception($"No existe el ejercicio {num}");
                }

                tcDatabase = Convert.ToString(num);
            }

            string value = "";
            _oAliasDB.TryGetValue(tcDatabase, out value);
            if(value == null)
            {
                value = $"[{tcDatabase}].dbo.";
            }

            return value;
        }

        public static string SQLDatabase(string tcTabla)
        {
            _ = string.Empty;
            return SQLDatabase("2024XP", tcTabla);
        }

        public static bool Escribir_En_Log_Analisis(string tcMensaje, Modo_Analisis teModo_Analisis = Modo_Analisis.Indeterminado, decimal tnTiempoRelativo = 0m, decimal tnTiempoAcumulado = 0m, string tcConsulta = "")
        {
            if(_lWritingAnalisis)
            {
                return true;
            }

            _lWritingAnalisis = true;
            int num = 0;
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            num = Convert.ToInt32(SQLValor("ewaplica", "upper(comunes)", DbComunes.Trim().ToUpper(), "codigo", "EUROWINSYS"));
            string empty = string.Empty;
            empty = Environment.MachineName.Trim();
            if(empty.Length > 15)
            {
                empty = empty.Substring(0, 15);
            }

            string empty2 = string.Empty;
            empty2 = ((!Convert.ToBoolean(_GetVariable("_Cargados_Diccionarios"))) ? Usuario_EW : Convert.ToString(_GetVariable("wc_usuario")));
            string tcTipo = string.Empty;
            switch(teModo_Analisis)
            {
                case Modo_Analisis.Manual:
                    tcTipo = "MAN";
                    break;
                case Modo_Analisis.Automatico:
                    tcTipo = "AUT";
                    break;
                case Modo_Analisis.Errores:
                    tcTipo = "ERR";
                    break;
                case Modo_Analisis.Consultas:
                    tcTipo = "SQL";
                    break;
                case Modo_Analisis.Traza:
                    tcTipo = "TRA";
                    break;
            }

            string tcPila = string.Empty;
            string tcProces = string.Empty;
            int tnLinea = 0;
            ObtenerPila(ref tcPila, ref tcProces, ref tnLinea);
            bool flag = false;
            string tcInfoThread = string.Empty;
            flag = Thread.CurrentThread.IsThreadPoolThread;
            if(flag)
            {
                tcInfoThread = "ThreadId nº : " + Thread.CurrentThread.ManagedThreadId;
                tcInfoThread = tcInfoThread + " ThreadName: " + Thread.CurrentThread.Name;
                tcInfoThread = tcInfoThread + " ThreadState: " + Thread.CurrentThread.ThreadState;
                tcInfoThread = tcInfoThread + " ThreadPool: " + Thread.CurrentThread.IsThreadPoolThread;
                tcInfoThread = tcInfoThread + " ThreadPriority: " + Thread.CurrentThread.Priority;
            }

            StackTrace stackTrace = new StackTrace(fNeedFileInfo: true);
            int num2 = 2;
            StackFrame frame = stackTrace.GetFrame(num2);
            while(frame.GetMethod().Module.Name.ToLower() == "sage.ew.db.dll" && num2 < 6)
            {
                num2++;
                frame = stackTrace.GetFrame(num2);
            }

            _ = frame.GetMethod().Name;
            string text = System.IO.Path.GetFileName(frame.GetFileName());
            if(string.IsNullOrEmpty(text))
            {
                text = "";
            }

            string name = frame.GetMethod().Module.Name;
            SQLChangeConnection(currentAlias);
            return Escribir_En_Log_Analisis(num, empty, empty2, tcTipo, name, text, tnTiempoRelativo, tnTiempoAcumulado, tcMensaje, tcConsulta, tcPila, flag, tcInfoThread);
        }

        public static bool SQLDatabaseExist(string tcDatabase)
        {
            tcDatabase = tcDatabase.ToUpper().Trim();
            return _oAliasDB.ContainsKey(tcDatabase);
        }

        public static bool SQLDatabaseExistStrict(string tcDatabase)
        {
            tcDatabase = tcDatabase.ToUpper().Trim();
            bool flag = _oAliasDB.ContainsKey(tcDatabase);
            if(flag)
            {
                DataTable dtTabla = new DataTable();
                _oAliasDB.TryGetValue(tcDatabase, out var value);
                value = value.Replace(".dbo.", "").Replace("[", "").Replace("]", "")
                    .ToUpper();
                SQLExec("SELECT DB_ID('" + value + "') AS ID", ref dtTabla);
                flag = ((dtTabla != null && dtTabla.Rows.Count > 0 && dtTabla.Rows[0]["ID"] != null && dtTabla.Rows[0]["ID"] != DBNull.Value) ? true : false);
            }

            return flag;
        }

        public static int SQLAnchuraCampo(string tcDatabaseLogica, string tcTabla, string tcCampo)
        {
            return _TablesInformationSchema(tcDatabaseLogica, tcTabla)._AnchuraCampo(tcCampo);
        }

        public static bool SQLExisteCampo(string tcDatabaseLogica, string tcTabla, string tcCampo)
        {
            return _TablesInformationSchema(tcDatabaseLogica, tcTabla)._ExisteCampo(tcCampo);
        }

        public static bool SQLCampoPermiteNulos(string tcDatabaseLogica, string tcTabla, string tcCampo)
        {
            return _TablesInformationSchema(tcDatabaseLogica, tcTabla)._PermiteNulos(tcCampo);
        }

        public static string SQLTipoCampo(string tcDatabaseLogica, string tcTabla, string tcCampo)
        {
            return ObtenerTipoCampoEstandar(_TablesInformationSchema(tcDatabaseLogica, tcTabla)._TipoCampo(tcCampo));
        }

        public static string SQLTipoCampoExtended(string tcDatabaseLogica, string tcTabla, string tcCampo)
        {
            return ObtenerTipoCampoEstandarExtended(_TablesInformationSchema(tcDatabaseLogica, tcTabla)._TipoCampoExtended(tcCampo));
        }

        public static string SQLCampoCollation(string tcDatabaseLogica, string tcTabla, string tcCampo)
        {
            return _TablesInformationSchema(tcDatabaseLogica, tcTabla)._Collation(tcCampo);
        }

        public static string _SQLRutaDataBd(string tcBaseDatos)
        {
            string result = "";
            if(string.IsNullOrWhiteSpace(tcBaseDatos))
            {
                return "";
            }

            tcBaseDatos = tcBaseDatos.Trim();
            DataTable dtTabla = new DataTable();
            if(SQLExec("SELECT physical_name FROM [" + tcBaseDatos + "].sys.database_files ", ref dtTabla) && dtTabla.Rows.Count >= 1)
            {
                result = Convert.ToString(dtTabla.Rows[0][0]);
                result = System.IO.Path.GetDirectoryName(result);
            }

            return result;
        }

        public static string _SQLRutaData()
        {
            string result = "";
            DataTable dtTabla = new DataTable();
            if(SQLExec("SELECT physical_name FROM master.sys.master_files WHERE database_id = 1 AND file_id = 1", ref dtTabla) && dtTabla.Rows.Count == 1)
            {
                result = Convert.ToString(dtTabla.Rows[0][0]);
                result = System.IO.Path.GetDirectoryName(result);
            }

            return result;
        }

        public static string _SQLRutaBackup()
        {
            string result = "";
            DataTable dtTabla = new DataTable();
            try
            {
                if(SQLExec("EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\\Microsoft\\MSSQLServer\\MSSQLServer',N'BackupDirectory'", ref dtTabla) && dtTabla.Rows.Count == 1)
                {
                    result = Convert.ToString(dtTabla.Rows[0][1]);
                }
            }
            catch
            {
                result = "";
            }

            return result;
        }

        public static List<string> SQLDatabaseTables(string tcDatabaseLogica)
        {
            bool flag = false;
            new SqlCommand();
            DataTable dtTabla = new DataTable();
            _ = string.Empty;
            string value = string.Empty;
            string empty = string.Empty;
            string empty2 = string.Empty;
            List<string> list = new List<string>();
            if(!string.IsNullOrEmpty(tcDatabaseLogica))
            {
                try
                {
                    SqlConnection sqlConnection = new SqlConnection(Conexion);
                    if(sqlConnection.State == ConnectionState.Open)
                    {
                        flag = true;
                    }
                    else
                    {
                        _SQLOpen(sqlConnection);
                        flag = false;
                    }

                    _oAliasDB.TryGetValue(tcDatabaseLogica.ToUpper(), out value);
                    value = value.Replace(".dbo.", ".INFORMATION_SCHEMA.");
                    SQLExec("SELECT table_name, table_type  FROM " + value + "TABLES ORDER BY table_name", ref dtTabla);
                    if(dtTabla != null && dtTabla.Rows.Count > 0)
                    {
                        foreach(DataRow row in dtTabla.Rows)
                        {
                            empty = Convert.ToString(row["table_name"]).ToUpper();
                            empty2 = Convert.ToString(row["table_type"]).ToUpper();
                            if(empty != "SYSDIAGRAMS" && empty2 == "BASE TABLE")
                            {
                                list.Add(empty);
                            }
                        }
                    }

                    if(!flag)
                    {
                        sqlConnection.Close();
                    }
                }
                catch(Exception toEx)
                {
                    Registrar_Error(toEx);
                }
            }

            return list;
        }

        public static List<string> _SQL_GetTablePrimaryKey(string tcBd, string tcTable)
        {
            bool flag = false;
            new SqlCommand();
            DataTable dtTabla = new DataTable();
            _ = string.Empty;
            string empty = string.Empty;
            List<string> result = new List<string>();
            if(string.IsNullOrEmpty(tcBd) || string.IsNullOrEmpty(tcTable))
            {
                return result;
            }

            try
            {
                SqlConnection sqlConnection = new SqlConnection(Conexion);
                if(sqlConnection.State == ConnectionState.Open)
                {
                    flag = true;
                }
                else
                {
                    _SQLOpen(sqlConnection);
                    flag = false;
                }

                empty = ParseDatabase(tcBd);
                empty = empty.Replace(".dbo.", ".INFORMATION_SCHEMA.");
                SQLExec("SELECT C.COLUMN_NAME FROM " + empty + "TABLE_CONSTRAINTS T JOIN " + empty + "CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME = T.CONSTRAINT_NAME WHERE C.TABLE_NAME = " + SQLString(tcTable) + " AND T.CONSTRAINT_TYPE = 'PRIMARY KEY' ", ref dtTabla);
                if(!flag)
                {
                    sqlConnection.Close();
                }
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
                return result;
            }

            if(dtTabla != null && dtTabla.Rows.Count > 0)
            {
                result = (from dr in dtTabla.Rows.OfType<DataRow>()
                          select dr.Field<string>("COLUMN_NAME")).ToList();
            }

            return result;
        }

        public static string SQLNvl(string tcCampo, object txValor)
        {
            return "coalesce(" + tcCampo + ", " + SQLString(txValor) + ")";
        }

        public static string SQLNvl(string tcCampoUno, string tcCampoDos)
        {
            return "coalesce(" + tcCampoUno + ", " + tcCampoDos + ")";
        }

        public static string SQLNullOrEmpty(string tcCampo)
        {
            return " ( " + tcCampo + " is null or " + tcCampo + " = ' ' ) ";
        }

        public static string SQLNotNullNotEmpty(string tcCampo)
        {
            return " ( " + tcCampo + " is not null and " + tcCampo + " != ' ' ) ";
        }

        public static string SQLRound(string tcExpresion, int tnDecimales)
        {
            return "round(" + tcExpresion + "," + tnDecimales + ")";
        }

        public static string SQLAlltrim(string tcCadena)
        {
            return "rtrim(ltrim(" + tcCadena + "))";
        }

        public static string SQLPadl(string tcCadena, int tnLongitud, char tcCaracterRelleno = ' ')
        {
            return "left(replicate('" + tcCaracterRelleno + "'," + tnLongitud + "-datalength(" + tcCadena + "))+" + tcCadena + "," + tnLongitud + ")";
        }

        public static string SQLPadr(string tcCadena, int tnLongitud, char tcCaracterRelleno = ' ')
        {
            return "left(" + tcCadena + "+replicate('" + tcCaracterRelleno + "'," + tnLongitud + "-datalength(tcCadena))," + tnLongitud + ")";
        }

        public static string SQLBit(string tcCadena)
        {
            return "CAST(MAX(CAST(" + tcCadena + " as INT)) AS BIT)";
        }

        public static string SQLTrue()
        {
            return "cast(1 as bit)";
        }

        public static string SQLFalse()
        {
            return "cast(0 as bit)";
        }

        public static string SQLAbs(string tcExpresion)
        {
            return "abs(" + tcExpresion.Trim() + ")";
        }

        public static string SQLIif(string tcExpreVerif, string tcExpreCierta, string tcExpreFalse)
        {
            return "case when (" + tcExpreVerif + ") then (" + tcExpreCierta + ") else (" + tcExpreFalse + ") end ";
        }

        public static string SQLCase(string tcExpreVerif, Dictionary<string, string> tdicCasos, string tcExprElse = "")
        {
            string text = "";
            text = " CASE (" + tcExpreVerif + ") ";
            foreach(KeyValuePair<string, string> tdicCaso in tdicCasos)
            {
                text = text + " WHEN (" + tdicCaso.Key + ") THEN (" + tdicCaso.Value + ") ";
            }

            if(!string.IsNullOrWhiteSpace(tcExprElse))
            {
                text = text + " ELSE " + tcExprElse + " END ";
            }

            return text;
        }

        public static DateTime SQLGetDate()
        {
            DateTime result = default(DateTime);
            DataTable dtTabla = new DataTable();
            if(SQLExec("select convert(nvarchar(10), getdate(),103) as Fecha ", ref dtTabla))
            {
                try
                {
                    result = Convert.ToDateTime(dtTabla.Rows[0]["Fecha"]);
                }
                catch(Exception)
                {
                    result = DateTime.Today;
                }

                dtTabla.Dispose();
            }

            return result;
        }

        public static DateTime SQLGetDateTime()
        {
            DateTime result = default(DateTime);
            DataTable dtTabla = new DataTable();
            if(SQLExec("select GetDate() AS Fecha ", ref dtTabla))
            {
                result = Convert.ToDateTime(dtTabla.Rows[0]["Fecha"]);
                dtTabla.Dispose();
            }

            return result;
        }

        public static bool Escribir_En_Log_Error(Modo_Registro teModo_Registro, Exception toEx, string tcMsgError_o_instrsql = "", int tnDuracionMiliSegundos = 0)
        {
            string empty = string.Empty;
            string empty2 = string.Empty;
            string text = string.Empty;
            string tcPila = string.Empty;
            string tcProces = string.Empty;
            string text2 = string.Empty;
            int num = 0;
            int num2 = 0;
            int num3 = 0;
            if(string.IsNullOrWhiteSpace(DbComunes))
            {
                return true;
            }

            int registro_Log_Consultas = Registro_Log_Consultas;
            bool registro_Errores = Registro_Errores;
            Registro_Log_Consultas = 0;
            Registro_Errores = false;
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            num3 = Convert.ToInt32(SQLValor("ewaplica", "upper(comunes)", DbComunes.Trim().ToUpper(), "codigo", "EUROWINSYS"));
            empty2 = ((!Convert.ToBoolean(_GetVariable("_Cargados_Diccionarios"))) ? Usuario_EW : Convert.ToString(_GetVariable("wc_usuario")));
            empty = Environment.MachineName.Trim();
            if(empty.Length > 15)
            {
                empty = empty.Substring(0, 15);
            }

            bool flag = teModo_Registro == Modo_Registro.Registro_Consulta;
            if(teModo_Registro == Modo_Registro.Registro_Error || flag)
            {
                if(toEx != null)
                {
                    if(toEx.Source != null)
                    {
                        tcProces = toEx.Source.Trim();
                        if(tcProces.Length > 50)
                        {
                            tcProces = tcProces.Substring(0, 50);
                        }
                    }

                    if(toEx.TargetSite != null)
                    {
                        text = toEx.TargetSite.Module.Name.Trim();
                        if(text.Length > 25)
                        {
                            text = text.Substring(0, 25);
                        }
                    }

                    tcPila = tcPila + "Origen: " + Environment.NewLine + Environment.NewLine + toEx.Source + Environment.NewLine + Environment.NewLine;
                    tcPila = tcPila + "Pila interna: " + Environment.NewLine + Environment.NewLine + toEx.StackTrace + Environment.NewLine + Environment.NewLine;
                    tcPila = tcPila + "Pila métodos: " + Environment.NewLine + Environment.NewLine;
                }

                text2 = tcMsgError_o_instrsql;
                num2 = (flag ? tnDuracionMiliSegundos : 0);
            }

            num = 0;
            ObtenerPila(ref tcPila, ref tcProces, ref num, flag);
            if(text2.Length > 220)
            {
                text2 = text2.Substring(0, 220);
            }

            bool result = SQLExec("insert into " + SQLDatabase("eurowinsys", "log_error") + " (aplica, terminal, usuari, tempshora, formulari, proces, linea, codi_error, missatge, pila) VALUES(" + SQLString(num3) + "," + SQLString(empty) + "," + SQLString(empty2) + ", getdate() , " + SQLString(text) + ", " + SQLString(tcProces) + ", " + SQLString(num) + ", " + SQLString(num2) + ", " + SQLString(text2) + "," + SQLString(tcPila) + ")");
            if(registro_Log_Consultas == 1)
            {
                Escribir_En_Log_Analisis(text2, Modo_Analisis.Errores, 0m, num2, tcProces);
            }

            Registro_Log_Consultas = registro_Log_Consultas;
            Registro_Errores = registro_Errores;
            SQLChangeConnection(currentAlias);
            return result;
        }

        public static bool SQLChangeConnection(string tcAlias, bool tlForzarCambio = false)
        {
            bool result = false;
            if(string.IsNullOrEmpty(tcAlias))
            {
                Error_Message = "No se ha indicado el alias a la Base de datos.";
                return false;
            }

            if(!string.IsNullOrWhiteSpace(CurrentAlias) && CurrentAlias.ToLower().Trim() == tcAlias.ToLower().Trim() && !tlForzarCambio)
            {
                return true;
            }

            try
            {
                if(!_oConexionDB.ContainsKey(tcAlias))
                {
                    tcAlias = tcAlias.ToLower();
                }

                if(!_oConexionDB.ContainsKey(tcAlias))
                {
                    tcAlias = tcAlias.ToUpper();
                }

                if(_oConexionDB.ContainsKey(tcAlias))
                {
                    Dictionary<string, object> value = new Dictionary<string, object>();
                    if(_oConexionDB.TryGetValue(tcAlias, out value))
                    {
                        Conexion = ((SqlConnection)value["_oConexion"]).ConnectionString;
                        _oAliasDB = new Dictionary<string, string>((Dictionary<string, string>)value["_oAliasDB"]);
                        CurrentAlias = tcAlias;
                        result = true;
                    }
                }
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
                result = false;
            }

            return result;
        }

        public static bool _SQLConnectMaster(string tcServer, string tcUser, string tcPassword, ref SqlConnection tsConnection)
        {
            bool result = false;
            try
            {
                string text = "Persist Security Info=True;Server=" + tcServer + ";database=master";
                text = ((!string.IsNullOrEmpty(tcUser) && !string.IsNullOrEmpty(tcPassword)) ? (text + ";Uid=" + tcUser + ";Pwd=" + tcPassword + ";") : (text + ";Trusted_Connection=yes;"));
                tsConnection = new SqlConnection(text);
                tsConnection.Open();
                result = true;
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
                result = false;
            }
            finally
            {
                if(tsConnection.State == ConnectionState.Open)
                {
                    tsConnection.Close();
                }
            }

            return result;
        }

        public static bool _DbBackup(string tcNombreBd, string tcCarpetaBackup)
        {
            return SQLExec("BACKUP DATABASE [" + tcNombreBd + "] TO  DISK = N'" + tcCarpetaBackup + "\\" + tcNombreBd + ".bak' WITH INIT, NAME = N'" + tcNombreBd + "-Completa Base de datos Copia de seguridad' ");
        }

        public static bool _DbRestore(string tcNombreViejo, string tcNombreNuevo, string tcCarpetaBackup, string tcPropietario)
        {
            DataTable dtTabla = new DataTable();
            string text = "";
            Error_Message = "";
            SQLExec("SELECT name, physical_name as fichero FROM [master].[sys].[master_files] where SUBSTRING(name,1,8)='" + tcNombreViejo + "' order by name", ref dtTabla);
            if(dtTabla.Rows.Count == 0)
            {
                Error_Message = "No se ha podido averiguar la ruta física de los archivos de la base de datos " + tcNombreViejo + ". Error: " + Error_Message;
                return false;
            }

            text = System.IO.Path.GetDirectoryName(Convert.ToString(dtTabla.Rows[0]["fichero"]).Trim());
            string tcSql = "RESTORE DATABASE [" + tcNombreNuevo + "] FROM DISK=N'" + tcCarpetaBackup + "\\" + tcNombreViejo + ".bak' WITH RECOVERY, MOVE '" + tcNombreViejo + "' TO '" + text + "\\" + tcNombreNuevo + ".mdf', MOVE '" + tcNombreViejo + "_dat' TO '" + text + "\\" + tcNombreNuevo + "_dat.ndf', MOVE '" + tcNombreViejo + "_idx' TO '" + text + "\\" + tcNombreNuevo + "_idx.ndf', MOVE '" + tcNombreViejo + "_log' TO '" + text + "\\" + tcNombreNuevo + "_log.ndf' ";
            Error_Message = "";
            if(!SQLExec(tcSql))
            {
                Error_Message = "La restauración de la base de datos " + tcNombreViejo + " a la nueva base de datos " + tcNombreNuevo + " no se ha realizado correctamente. Error: " + Error_Message;
                return false;
            }

            string tcSql2 = "ALTER AUTHORIZATION ON DATABASE::[" + tcNombreNuevo + "] TO " + tcPropietario;
            Error_Message = "";
            if(!SQLExec(tcSql2))
            {
                Error_Message = "No se ha podido cambiar el OWNER de la nueva base de datos " + tcNombreNuevo + " al usuario " + tcPropietario + ". Error: " + Error_Message;
                return false;
            }

            string tcSql3 = "ALTER DATABASE [" + tcNombreNuevo + "]  SET AUTO_CLOSE OFF WITH NO_WAIT";
            Error_Message = "";
            if(!SQLExec(tcSql3))
            {
                Error_Message = "No se ha podido cambiar la propiedad AUTO_CLOSE de la nueva base de datos " + tcNombreNuevo + ". Error: " + Error_Message;
                return false;
            }

            string tcSql4 = "ALTER DATABASE [" + tcNombreNuevo + "] MODIFY FILE  (NAME = '" + tcNombreViejo + "', NEWNAME =  '" + tcNombreNuevo + "')";
            Error_Message = "";
            if(!SQLExec(tcSql4))
            {
                Error_Message = "No se ha podido cambiar la propiedad NAME/NEWNAME del fichero mdf de la nueva base de datos " + tcNombreNuevo + ". Error: " + Error_Message;
                return false;
            }

            string tcSql5 = "ALTER DATABASE [" + tcNombreNuevo + "] MODIFY FILE  (NAME = '" + tcNombreViejo + "_dat', NEWNAME =  '" + tcNombreNuevo + "_dat')";
            Error_Message = "";
            if(!SQLExec(tcSql5))
            {
                Error_Message = "No se ha podido cambiar la propiedad NAME/NEWNAME del fichero _dat de la nueva base de datos " + tcNombreNuevo + ". Error: " + Error_Message;
                return false;
            }

            string tcSql6 = "ALTER DATABASE [" + tcNombreNuevo + "] MODIFY FILE  (NAME = '" + tcNombreViejo + "_idx', NEWNAME =  '" + tcNombreNuevo + "_idx')";
            Error_Message = "";
            if(!SQLExec(tcSql6))
            {
                Error_Message = "No se ha podido cambiar la propiedad NAME/NEWNAME del fichero _idx de la nueva base de datos " + tcNombreNuevo + ". Error: " + Error_Message;
                return false;
            }

            string tcSql7 = "ALTER DATABASE [" + tcNombreNuevo + "] MODIFY FILE  (NAME = '" + tcNombreViejo + "_log', NEWNAME =  '" + tcNombreNuevo + "_log')";
            Error_Message = "";
            if(!SQLExec(tcSql7))
            {
                Error_Message = "No se ha podido cambiar la propiedad NAME/NEWNAME del fichero _log de la nueva base de datos " + tcNombreNuevo + ". Error: " + Error_Message;
                return false;
            }

            return true;
        }

        public static bool _DbRestore(string tcNombreBak, string tcNombreNuevo, string tcCarpetaBackup, string tcPropietario, string tcRutaBds)
        {
            string text = tcNombreBak;
            if(string.IsNullOrEmpty(tcRutaBds))
            {
                Error_Message = "La ruta a la base de datos no existe. Error: " + Error_Message;
                return false;
            }

            DataTable dtTabla = new DataTable();
            SQLExec("RESTORE FILELISTONLY FROM DISK=N'" + tcCarpetaBackup + "\\" + tcNombreBak + ".bak'", ref dtTabla);
            DataRow[] array = dtTabla.Select("PhysicalName LIKE '*.mdf' OR PhysicalName LIKE '*.MDF'");
            if(array.Length != 0)
            {
                text = Convert.ToString(array[0]["LogicalName"]);
            }

            string tcSql = "RESTORE DATABASE [" + tcNombreNuevo + "] FROM DISK=N'" + tcCarpetaBackup + "\\" + tcNombreBak + ".bak' WITH RECOVERY, MOVE '" + text + "' TO '" + tcRutaBds + "\\" + tcNombreNuevo + ".mdf', MOVE '" + text + "_dat' TO '" + tcRutaBds + "\\" + tcNombreNuevo + "_dat.ndf', MOVE '" + text + "_idx' TO '" + tcRutaBds + "\\" + tcNombreNuevo + "_idx.ndf', MOVE '" + text + "_log' TO '" + tcRutaBds + "\\" + tcNombreNuevo + "_log.ndf' ";
            Error_Message = "";
            if(!SQLExec(tcSql))
            {
                Error_Message = "La restauración de la base de datos " + tcNombreBak + " a la nueva base de datos " + tcNombreNuevo + " no se ha realizado correctamente. Error: " + Error_Message;
                return false;
            }

            SQLExec("USE [" + tcNombreNuevo + "]; IF EXISTS (SELECT name FROM [sys].[database_principals] WHERE[type] = 'S' AND name = N'" + tcPropietario + "') BEGIN DROP USER " + tcPropietario + " END");
            string tcSql2 = "ALTER AUTHORIZATION ON DATABASE::[" + tcNombreNuevo + "] TO " + tcPropietario;
            Error_Message = "";
            if(!SQLExec(tcSql2))
            {
                Error_Message = "No se ha podido cambiar el OWNER de la nueva base de datos " + tcNombreNuevo + " al usuario " + tcPropietario + ". Error: " + Error_Message;
                return false;
            }

            string tcSql3 = "ALTER DATABASE [" + tcNombreNuevo + "]  SET AUTO_CLOSE OFF WITH NO_WAIT";
            Error_Message = "";
            if(!SQLExec(tcSql3))
            {
                Error_Message = "No se ha podido cambiar la propiedad AUTO_CLOSE de la nueva base de datos " + tcNombreNuevo + ". Error: " + Error_Message;
                return false;
            }

            if(text.Trim() != tcNombreNuevo.Trim())
            {
                string tcSql4 = "ALTER DATABASE [" + tcNombreNuevo + "] MODIFY FILE  (NAME = '" + text + "', NEWNAME =  '" + tcNombreNuevo + "')";
                Error_Message = "";
                if(!SQLExec(tcSql4))
                {
                    Error_Message = "No se ha podido cambiar la propiedad NAME/NEWNAME del fichero mdf de la nueva base de datos " + tcNombreNuevo + ". Error: " + Error_Message;
                    return false;
                }

                string tcSql5 = "ALTER DATABASE [" + tcNombreNuevo + "] MODIFY FILE  (NAME = '" + text + "_dat', NEWNAME =  '" + tcNombreNuevo + "_dat')";
                Error_Message = "";
                if(!SQLExec(tcSql5))
                {
                    Error_Message = "No se ha podido cambiar la propiedad NAME/NEWNAME del fichero _dat de la nueva base de datos " + tcNombreNuevo + ". Error: " + Error_Message;
                    return false;
                }

                string tcSql6 = "ALTER DATABASE [" + tcNombreNuevo + "] MODIFY FILE  (NAME = '" + text + "_idx', NEWNAME =  '" + tcNombreNuevo + "_idx')";
                Error_Message = "";
                if(!SQLExec(tcSql6))
                {
                    Error_Message = "No se ha podido cambiar la propiedad NAME/NEWNAME del fichero _idx de la nueva base de datos " + tcNombreNuevo + ". Error: " + Error_Message;
                    return false;
                }

                string tcSql7 = "ALTER DATABASE [" + tcNombreNuevo + "] MODIFY FILE  (NAME = '" + text + "_log', NEWNAME =  '" + tcNombreNuevo + "_log')";
                Error_Message = "";
                if(!SQLExec(tcSql7))
                {
                    Error_Message = "No se ha podido cambiar la propiedad NAME/NEWNAME del fichero _log de la nueva base de datos " + tcNombreNuevo + ". Error: " + Error_Message;
                    return false;
                }
            }

            return true;
        }

        public static bool _DbRestore(string tcNombreDb, string tcCarpetaBackup, string tcPropietario)
        {
            bool flag = false;
            new DataTable();
            string text = "";
            Error_Message = "";
            if(!DbRutaBaseDatos())
            {
                Error_Message = "No se ha podido averiguar la ruta física de los archivos de la base de datos " + tcNombreDb + ". Error: " + Error_Message;
                return false;
            }

            text = System.IO.Path.GetDirectoryName(_RutaBaseDatos);
            flag = SQLExec("USE master; ");
            flag = SQLExec("ALTER DATABASE [" + tcNombreDb + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; ALTER DATABASE [" + tcNombreDb + "] SET MULTI_USER; ");
            string tcSql = "RESTORE DATABASE [" + tcNombreDb + "] FROM DISK=N'" + tcCarpetaBackup + "\\" + tcNombreDb + ".bak' WITH RECOVERY, MOVE '" + tcNombreDb + "' TO '" + text + "\\" + tcNombreDb + ".mdf', MOVE '" + tcNombreDb + "_dat' TO '" + text + "\\" + tcNombreDb + "_dat.ndf', MOVE '" + tcNombreDb + "_idx' TO '" + text + "\\" + tcNombreDb + "_idx.ndf', MOVE '" + tcNombreDb + "_log' TO '" + text + "\\" + tcNombreDb + "_log.ndf' ";
            Error_Message = "";
            if(!SQLExec(tcSql))
            {
                Error_Message = "La restauración de la base de datos " + tcNombreDb + " no se ha realizado correctamente. Error: " + Error_Message;
                return false;
            }

            string tcSql2 = "ALTER AUTHORIZATION ON DATABASE::[" + tcNombreDb + "] TO " + tcPropietario;
            Error_Message = "";
            if(!SQLExec(tcSql2))
            {
                Error_Message = "No se ha podido cambiar el OWNER de la nueva base de datos " + tcNombreDb + " al usuario " + tcPropietario + ". Error: " + Error_Message;
                return false;
            }

            string tcSql3 = "ALTER DATABASE [" + tcNombreDb + "]  SET AUTO_CLOSE OFF WITH NO_WAIT";
            Error_Message = "";
            flag = SQLExec(tcSql3);
            if(!flag)
            {
                Error_Message = "No se ha podido cambiar la propiedad AUTO_CLOSE de la nueva base de datos " + tcNombreDb + ". Error: " + Error_Message;
                return false;
            }

            return flag;
        }

        public static string _ValorSQL(string tcSql)
        {
            DataTable dtTabla = new DataTable();
            string result = "";
            try
            {
                if(!_Contiene_Palabras_Reservadas(tcSql))
                {
                    string text = tcSql;
                    foreach(Match item in Regex.Matches(text, "\\s+\\w+!\\w+\\s+"))
                    {
                        int num = item.Value.IndexOf('!');
                        string tcDatabase = item.Value.Substring(0, num);
                        string tcTabla = item.Value.Substring(num + 1);
                        text = Regex.Replace(text, item.Value.Trim(), SQLDatabase(tcDatabase, tcTabla));
                    }

                    if(SQLExec(text, ref dtTabla))
                    {
                        result = ((dtTabla.Rows.Count > 0) ? Convert.ToString(dtTabla.Rows[0][0]) : string.Empty);
                    }
                }
            }
            catch(Exception toEx)
            {
                dtTabla = null;
                result = "";
                Registrar_Error(toEx);
                return null;
            }

            return result;
        }

        public static string _ValorSQL(string tcTabla, string[] tcWhere, string[] taCampos, string tcDatabase = "2024XP")
        {
            string empty = string.Empty;
            bool flag = true;
            DataTable dtTabla = new DataTable();
            string text = "";
            try
            {
                string text2 = "";
                string arg = "";
                foreach(string arg2 in taCampos)
                {
                    text2 += $"{arg} {arg2}";
                    arg = " , ";
                }

                empty = $"SELECT {text2} FROM {SQLDatabase(tcDatabase, tcTabla)}";
                string text3 = "";
                if(tcWhere.Length == 1 && tcWhere[0] == string.Empty)
                {
                    flag = false;
                }

                if(flag)
                {
                    text3 = " WHERE ";
                    for(int j = 0; j < tcWhere.Length; j++)
                    {
                        if(j > 0)
                        {
                            text3 += " AND ";
                        }

                        text3 += tcWhere[j];
                    }

                    if(_Contiene_Palabras_Reservadas(text3))
                    {
                        text3 = "";
                    }

                    empty += text3;
                }

                if(!SQLExec(empty, ref dtTabla))
                {
                    return null;
                }

                for(int k = 0; k < dtTabla.Columns.Count; k++)
                {
                    text = ((dtTabla.Rows.Count > 0) ? (text + Convert.ToString(dtTabla.Rows[0][k])) : Convert.ToString(_Valor_Defecto(dtTabla.Columns[k].DataType)));
                }

                return text;
            }
            catch(Exception toEx)
            {
                dtTabla = null;
                text = "";
                Registrar_Error(toEx);
                return null;
            }
        }

        public static string _ValorSqlMultiReg(string tcTabla, string[] tcWhere, string[] taCampos, string tcSeparadorCamposResultado, string tcDatabase = "2024XP")
        {
            string empty = string.Empty;
            bool flag = true;
            DataTable dtTabla = new DataTable();
            string text = "";
            try
            {
                string text2 = "";
                string arg = "";
                foreach(string arg2 in taCampos)
                {
                    text2 += $"{arg} {arg2}";
                    arg = " , ";
                }

                empty = $"SELECT {text2} FROM {SQLDatabase(tcDatabase, tcTabla)}";
                string text3 = "";
                if(tcWhere.Length == 1 && tcWhere[0] == string.Empty)
                {
                    flag = false;
                }

                if(flag)
                {
                    text3 = " WHERE ";
                    for(int j = 0; j < tcWhere.Length; j++)
                    {
                        if(j > 0)
                        {
                            text3 += " AND ";
                        }

                        text3 += tcWhere[j];
                    }

                    if(_Contiene_Palabras_Reservadas(text3))
                    {
                        text3 = "";
                    }

                    empty += text3;
                }

                if(!SQLExec(empty, ref dtTabla) || dtTabla.Rows.Count <= 0)
                {
                    return null;
                }

                string text4 = "";
                foreach(DataRow row in dtTabla.Rows)
                {
                    text += text4;
                    text += string.Join(tcSeparadorCamposResultado, row.ItemArray);
                    text4 = Environment.NewLine;
                }

                return text;
            }
            catch(Exception toEx)
            {
                dtTabla = null;
                text = "";
                Registrar_Error(toEx);
                return null;
            }
        }

        public static bool _Contiene_Palabras_Reservadas(string tcTexto)
        {
            if(!string.IsNullOrWhiteSpace(tcTexto) && _cExclusionWords.Any((string lcWord) => tcTexto.ToUpper().Contains(lcWord.ToUpper())))
            {
                return true;
            }

            return false;
        }

        public static bool _DBExist(SqlConnection tsConnection, string tcNombreDB)
        {
            bool result = false;
            try
            {
                SqlCommand sqlCommand = new SqlCommand($"SELECT database_id FROM sys.databases WHERE Name = '{tcNombreDB}'", tsConnection);
                tsConnection.Open();
                result = Convert.ToInt32(sqlCommand.ExecuteScalar()) > 0;
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
                result = false;
            }
            finally
            {
                if(tsConnection.State == ConnectionState.Open)
                {
                    tsConnection.Close();
                }
            }

            return result;
        }

        public static bool _DBCreate(string tcNombreDB)
        {
            if(string.IsNullOrWhiteSpace(tcNombreDB))
            {
                return false;
            }

            string text = _SQLRutaDataBd(DbComunes);
            if(string.IsNullOrWhiteSpace(text))
            {
                text = _SQLRutaData();
            }

            text = text.Trim();
            if(!text.EndsWith("\\"))
            {
                text = text.Trim() + "\\";
            }

            string text2 = "CREATE DATABASE [" + tcNombreDB + "] ON  PRIMARY ( NAME = N'" + tcNombreDB + "', FILENAME = N'" + text + tcNombreDB + ".mdf' , FILEGROWTH = 1024KB ),   FILEGROUP [eurowind] ( NAME = N'" + tcNombreDB + "_dat', FILENAME = N'" + text + tcNombreDB + "_dat.ndf' , FILEGROWTH = 1024KB ),   FILEGROUP [eurowini] ( NAME = N'" + tcNombreDB + "_idx', FILENAME = N'" + text + tcNombreDB + "_Idx.ndf' , FILEGROWTH = 1024KB )   LOG ON ( NAME = N'" + tcNombreDB + "_log', FILENAME = N'" + text + tcNombreDB + "_log.ldf' , FILEGROWTH = 10% );  ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET ANSI_NULL_DEFAULT OFF; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET ANSI_NULLS OFF; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET ANSI_PADDING ON; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET ANSI_WARNINGS OFF; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET ARITHABORT OFF; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET AUTO_CLOSE OFF; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] COLLATE Modern_Spanish_CI_AI; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET AUTO_CREATE_STATISTICS ON; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET AUTO_SHRINK OFF; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET AUTO_UPDATE_STATISTICS ON; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET CURSOR_CLOSE_ON_COMMIT OFF; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET CURSOR_DEFAULT GLOBAL; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET CONCAT_NULL_YIELDS_NULL OFF; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET NUMERIC_ROUNDABORT OFF; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET QUOTED_IDENTIFIER OFF;";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET RECURSIVE_TRIGGERS OFF; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET AUTO_UPDATE_STATISTICS_ASYNC OFF; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET DATE_CORRELATION_OPTIMIZATION OFF; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET PARAMETERIZATION SIMPLE; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET READ_WRITE; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET RECOVERY SIMPLE ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET MULTI_USER; ";
            text2 = text2 + "ALTER DATABASE [" + tcNombreDB + "] SET PAGE_VERIFY CHECKSUM ;";
            text2 = text2 + "IF NOT EXISTS (SELECT name FROM [" + tcNombreDB + "].sys.filegroups WHERE is_default=1 AND name = N'PRIMARY') ALTER DATABASE [" + tcNombreDB + "] MODIFY FILEGROUP [PRIMARY] DEFAULT ";
            bool num = SQLExec(text2);
            if(num)
            {
                SQLExec("ALTER DATABASE [" + tcNombreDB + "] SET PARAMETERIZATION FORCED WITH NO_WAIT");
            }

            return num;
        }

        public static bool _DBCreate(SqlConnection tsConnection, string tcNombreDB, bool tlAutoClose = false, string tcCollation = "")
        {
            bool result = false;
            string text = "CREATE DATABASE [" + tcNombreDB + "]";
            if(!string.IsNullOrEmpty(tcCollation))
            {
                text = text + " COLLATE " + tcCollation;
            }

            text += ";";
            SqlCommand sqlCommand = new SqlCommand(text, tsConnection);
            try
            {
                tsConnection.Open();
                sqlCommand.ExecuteNonQuery();
                if(tlAutoClose)
                {
                    text = "ALTER DATABASE [" + tcNombreDB + "] SET AUTO_CLOSE OFF;";
                    sqlCommand = new SqlCommand(text, tsConnection);
                    sqlCommand.ExecuteNonQuery();
                }

                result = true;
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
                result = false;
            }
            finally
            {
                if(tsConnection.State == ConnectionState.Open)
                {
                    tsConnection.Close();
                }
            }

            return result;
        }

        public static string _ServerColation()
        {
            DataTable dtTabla = new DataTable();
            if(SQLExec("SELECT CONVERT (varchar, SERVERPROPERTY('collation'))", ref dtTabla) && dtTabla.Rows.Count > 0)
            {
                return Convert.ToString(dtTabla.Rows[0][0]).TrimEnd();
            }

            return "Modern_Spanish_CI_AI";
        }

        public static bool _DBRemove(SqlConnection tsConnection, string tcNombreDB)
        {
            bool flag = false;
            flag = SQLExec("USE master; ");
            flag = SQLExec("ALTER DATABASE [" + tcNombreDB + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; ALTER DATABASE [" + tcNombreDB + "] SET MULTI_USER; ");
            SqlCommand sqlCommand = new SqlCommand("DROP DATABASE [" + tcNombreDB + "];", tsConnection);
            try
            {
                tsConnection.Open();
                sqlCommand.ExecuteNonQuery();
                flag = true;
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
                flag = false;
            }
            finally
            {
                if(tsConnection.State == ConnectionState.Open)
                {
                    tsConnection.Close();
                }
            }

            return flag;
        }

        public static string _DBIsnull(string tcCampo)
        {
            string text = "";
            if(string.IsNullOrEmpty(tcCampo))
            {
                return "";
            }

            return tcCampo + " is null";
        }

        public static bool _SQLVolcadoMasivo(DataTable tdtDatos, string tcTabla, int tnTimeOut = 2700)
        {
            bool result = true;
            DataTable dtTabla = new DataTable();
            Dictionary<string, string> dictionary = new Dictionary<string, string>();
            Dictionary<string, string> dictionary2 = new Dictionary<string, string>();
            string tcSql = $" SELECT TOP(1) * FROM  {tcTabla} WHERE 1 = 1 ";
            try
            {
                SQLExec(tcSql, ref dtTabla);
                foreach(DataColumn column in dtTabla.Columns)
                {
                    if(!dictionary.Keys.Contains(column.ColumnName.ToLower()))
                    {
                        dictionary.Add(column.ColumnName.ToLower(), column.ColumnName);
                    }
                    else
                    {
                        Escribir_En_Log_Analisis("Clave diccionario SQL duplicada " + column.ColumnName, Modo_Analisis.Manual);
                    }
                }

                foreach(DataColumn column2 in tdtDatos.Columns)
                {
                    if(!dictionary2.Keys.Contains(column2.ColumnName.ToLower()))
                    {
                        dictionary2.Add(column2.ColumnName.ToLower(), column2.ColumnName);
                    }
                    else
                    {
                        Escribir_En_Log_Analisis("Clave diccionario datos duplicada " + column2.ColumnName, Modo_Analisis.Manual);
                    }
                }

                foreach(DataRow row in tdtDatos.Rows)
                {
                    foreach(DataColumn column3 in tdtDatos.Columns)
                    {
                        if(column3.DataType.Name == "Decimal" && (row[column3] == DBNull.Value || row[column3] == null))
                        {
                            row[column3] = 0m;
                        }
                    }
                }

                SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(Conexion);
                sqlBulkCopy.BulkCopyTimeout = tnTimeOut;
                sqlBulkCopy.DestinationTableName = tcTabla;
                foreach(KeyValuePair<string, string> item in dictionary2)
                {
                    if(dictionary.ContainsKey(item.Key))
                    {
                        sqlBulkCopy.ColumnMappings.Add(dictionary2[item.Key].ToString(), dictionary[item.Key].ToString());
                    }
                }

                sqlBulkCopy.WriteToServer(tdtDatos);
                sqlBulkCopy.Close();
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
                result = false;
            }
            finally
            {
                dictionary2 = null;
                dictionary = null;
            }

            return result;
        }

        public static bool SQLExisteTabla(string tcNombreTabla, string tcNombreBBDD = "EUROWINSYS")
        {
            return _DBsInformationSchema(tcNombreBBDD)._ExisteTabla(tcNombreTabla);
        }

        public static bool DbRutaBaseDatos()
        {
            new DataTable();
            DataTable dtTabla = new DataTable();
            if(SQLExec("SELECT SUBSTRING(physical_name, 1, CHARINDEX(N'master.mdf', LOWER(physical_name)) - 1) As dirdata FROM master.sys.master_files WHERE database_id = 1 AND file_id = 1", ref dtTabla) && dtTabla.Rows.Count > 0)
            {
                _RutaBaseDatos = dtTabla.Rows[0]["dirdata"].ToString();
                if(string.IsNullOrWhiteSpace(_RutaBaseDatos))
                {
                    Error_Message = "No se encontrado el directorio de datos por defecto.";
                    return false;
                }

                return true;
            }

            Error_Message = "No se encontrado el directorio de datos por defecto.";
            return false;
        }

        public static bool _SqlDatabase2024XPExists(string tcDatabase)
        {
            bool result = false;
            tcDatabase = tcDatabase.ToUpper().Trim();
            if(tcDatabase.Contains("2024XP") && tcDatabase.Contains('-') && tcDatabase.Length > tcDatabase.IndexOf('-') + 1)
            {
                int result2 = 0;
                if(int.TryParse(tcDatabase.Substring(tcDatabase.IndexOf('-') + 1), out result2))
                {
                    int num = Convert.ToInt32(Ejercicio_EW) - result2;
                    if(_oAliasDB.ContainsKey(Convert.ToString(num)))
                    {
                        tcDatabase = Convert.ToString(num);
                        result = true;
                    }
                }
            }

            return result;
        }

        public static Dictionary<string, object> SQLREGValor(string tcTabla, string tcWhere, string tcClave, string tcDatabase = "2024XP")
        {
            string[] tcWhere2 = new string[1] { tcWhere };
            string[] array = new string[1] { tcClave };
            object[] tcClave2 = array;
            return SQLREGValor(tcTabla, tcWhere2, tcClave2, tcDatabase);
        }

        public static Dictionary<string, object> SQLREGValor(string tcTabla, string[] tcWhere, object[] tcClave, string tcDatabase = "2024XP")
        {
            new Dictionary<string, object>();
            return _SQLValor(tcTabla, tcWhere, tcClave, tcDatabase);
        }

        public static object SQLValor(string tcTabla, string tcWhere, string tcClave, string tcValor, string tcDatabase = "2024XP")
        {
            MessageBox.Show("AT: \n\nReplicatedDB.SQLValor(\"PAISES\", \"CODIGO\", pcPais, \"CODIGO\", \"COMUNES\").ToString()");
            string[] tcWhere2 = new string[1] { tcWhere };
            string[] array = new string[1] { tcClave };
            object[] tcClave2 = array;
            return SQLValor(tcTabla, tcWhere2, tcClave2, tcValor, tcDatabase);
        }

        public static object SQLValor(string tcTabla, string[] tcWhere, object[] tcClave, string tcValor, string tcDatabase = "2024XP")
        {
            MessageBox.Show("AT: \n\npublic static object SQLValor(string tcTabla, string[] tcWhere, object[] tcClave, string tcValor, string tcDatabase = \"2024XP\")");
            Dictionary<string, object> dictionary = new Dictionary<string, object>();
            object value = null;
            dictionary = _SQLValor(tcTabla, new string[1] { tcValor }, tcWhere, tcClave, tcDatabase);
            if(dictionary != null && dictionary.TryGetValue(tcValor.ToLower(), out value))
            {
                return value;
            }
            return null;
        }

        public static Dictionary<string, object> SQLValor(string tcTabla, string[] tcWhere, object[] tcClave, string[] tcValor, string tcDatabase = "2024XP")
        {
            return _SQLValor(tcTabla, tcValor, tcWhere, tcClave, tcDatabase);
        }

        public static void SQLBegin()
        {
            try
            {
                if(!_oTrans.ContainsKey(Thread.CurrentThread.ManagedThreadId))
                {
                    SqlConnection sqlConnection = new SqlConnection(Conexion);
                    _SQLOpen(sqlConnection);
                    SqlTransaction oTransaccion = sqlConnection.BeginTransaction(IsolationLevel.ReadCommitted);
                    Transac value = default(Transac);
                    value._oConexionTransaccion = sqlConnection;
                    value._oTransaccion = oTransaccion;
                    value._nId = Thread.CurrentThread.ManagedThreadId;
                    _oTrans.Add(Thread.CurrentThread.ManagedThreadId, value);
                    _QueryManager.CambiarTipoConsultasA(eTipoQuery.Transaccional);
                }
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
            }
        }

        public static void SQLBegin(string tcMensajeLog)
        {
            Escribir_En_Log_Analisis(tcMensajeLog);
            SQLBegin();
        }

        public static void SQLCommit()
        {
            try
            {
                if(_oTrans.ContainsKey(Thread.CurrentThread.ManagedThreadId))
                {
                    _oTrans[Thread.CurrentThread.ManagedThreadId]._oTransaccion.Commit();
                    _oTrans[Thread.CurrentThread.ManagedThreadId]._oConexionTransaccion.Close();
                }
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
            }
            finally
            {
                if(_oTrans.ContainsKey(Thread.CurrentThread.ManagedThreadId))
                {
                    if(_oTrans[Thread.CurrentThread.ManagedThreadId]._oConexionTransaccion.State == ConnectionState.Open)
                    {
                        _oTrans[Thread.CurrentThread.ManagedThreadId]._oConexionTransaccion.Close();
                    }

                    _oTrans.Remove(Thread.CurrentThread.ManagedThreadId);
                }

                if(_oTrans.Keys.Count == 0)
                {
                    _QueryManager.RestablecerTipoConsultaAnterior();
                }
            }
        }

        public static void SQLCommit(string tcMensajeLog)
        {
            SQLCommit();
            Escribir_En_Log_Analisis(tcMensajeLog);
        }

        public static void SQLRollback()
        {
            try
            {
                if(_oTrans.ContainsKey(Thread.CurrentThread.ManagedThreadId))
                {
                    _oTrans[Thread.CurrentThread.ManagedThreadId]._oTransaccion.Rollback();
                    _oTrans[Thread.CurrentThread.ManagedThreadId]._oConexionTransaccion.Close();
                }
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
            }
            finally
            {
                if(_oTrans.ContainsKey(Thread.CurrentThread.ManagedThreadId))
                {
                    if(_oTrans[Thread.CurrentThread.ManagedThreadId]._oConexionTransaccion.State == ConnectionState.Open)
                    {
                        _oTrans[Thread.CurrentThread.ManagedThreadId]._oConexionTransaccion.Close();
                    }

                    _oTrans.Remove(Thread.CurrentThread.ManagedThreadId);
                }

                if(_oTrans.Keys.Count == 0)
                {
                    _QueryManager.RestablecerTipoConsultaAnterior();
                }
            }
        }

        public static void SQLRollback(string tcMensajeLog)
        {
            SQLRollback();
            Escribir_En_Log_Analisis(tcMensajeLog);
        }

        public static void SQLPredeterminar2024XP(string tcEjercicio)
        {
            string value = string.Empty;
            tcEjercicio = tcEjercicio.Trim();
            if(_oAliasDB.TryGetValue(tcEjercicio, out value))
            {
                _oAliasDB["2024XP"] = value;
                Ejercicio_EW = tcEjercicio;
                if(!string.IsNullOrWhiteSpace(CurrentAlias) && _oConexionDB.ContainsKey(CurrentAlias))
                {
                    Dictionary<string, object> value2 = new Dictionary<string, object>();
                    _oConexionDB.TryGetValue(CurrentAlias, out value2);
                    value2.Remove("_oAliasDB");
                    Dictionary<string, string> value3 = new Dictionary<string, string>(_oAliasDB);
                    value2.Add("_oAliasDB", value3);
                    _oConexionDB.Remove(CurrentAlias);
                    _oConexionDB.Add(CurrentAlias, value2);
                    _dicCacheSchema.Clear();
                }
            }
        }

        public static void Registrar_Traza(string tcExtraInfo = "", string tcExtraMemoInfo = "", int tnDuracionMiliSegundos = 0)
        {
            if(string.IsNullOrWhiteSpace(tcExtraInfo) || !Convert.ToBoolean(_GetVariable("_lDebugMode")))
            {
                return;
            }

            if(_oConexionDB != null && _oConexionDB.Count > 0)
            {
                Escribir_En_Log_Analisis(tcExtraInfo, Modo_Analisis.Traza, 0m, tnDuracionMiliSegundos, tcExtraMemoInfo);
                return;
            }

            string text = Environment.MachineName.Trim();
            if(text.Length > 15)
            {
                text = text.Substring(0, 15);
            }

            string tcPila = string.Empty;
            string tcProces = string.Empty;
            int tnLinea = 0;
            ObtenerPila(ref tcPila, ref tcProces, ref tnLinea);
            tcPila = Environment.NewLine + Environment.NewLine + tcPila;
            if(!string.IsNullOrWhiteSpace(tcExtraMemoInfo))
            {
                tcExtraMemoInfo = Environment.NewLine + Environment.NewLine + tcExtraMemoInfo;
            }

            EventLog eventLog = new EventLog("Application");
            eventLog.Source = "Application";
            eventLog.WriteEntry(string.Format("{0} : {1} : {2} : {3} {4} {5}", "TRAZA " + System.IO.Path.GetFileName(Application.ExecutablePath).Trim(), DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), text, tcExtraInfo, tcExtraMemoInfo, tcPila), EventLogEntryType.Information);
        }

        public static void Registrar_Sql(string tcInstruccionSql = "", int tnDuracionMiliSegundos = 0)
        {
            if(!string.IsNullOrWhiteSpace(tcInstruccionSql) && Registro_Log_Consultas == 1)
            {
                string currentAlias = CurrentAlias;
                SQLChangeConnection("eurowin");
                Escribir_En_Log_Analisis(tcInstruccionSql, Modo_Analisis.Consultas, 0m, tnDuracionMiliSegundos, tcInstruccionSql);
                SQLChangeConnection(currentAlias);
            }
        }

        public static void Registrar_Consultas(string tcInstruccionSql = "")
        {
            if(_lRegistro_Consulta_Recursivo || _dicRegistroConsultas.Count == 0 || string.IsNullOrWhiteSpace(tcInstruccionSql))
            {
                return;
            }

            string text = tcInstruccionSql.ToLower().Trim();
            if(text.IndexOf("select") == 0)
            {
                return;
            }

            int num = 0;
            int num2 = 0;
            string text2 = string.Empty;
            string text3 = string.Empty;
            string empty = string.Empty;
            string text4 = string.Empty;
            if(text.Length >= 6)
            {
                text4 = text.Substring(0, 6);
            }

            switch(text4)
            {
                case "insert":
                    num2 = text.IndexOf("(");
                    if(num2 < 0)
                    {
                        num2 = num;
                        text3 = text.Substring(num);
                    }
                    else
                    {
                        text3 = text.Substring(num, num2 - num);
                    }

                    num = num2;
                    num2 = text.IndexOf("values");
                    if(num2 < 0)
                    {
                        num2 = num;
                        text2 = text.Substring(num);
                    }
                    else
                    {
                        text2 = text.Substring(num, num2 - num);
                    }

                    break;
                case "update":
                    num = num2;
                    num2 = text.IndexOf("set");
                    if(num2 < 0)
                    {
                        num2 = num;
                        text3 = text.Substring(num);
                    }
                    else
                    {
                        text3 = text.Substring(num, num2 - num);
                    }

                    num2 = text.IndexOf("where");
                    if(num2 < 0)
                    {
                        num2 = num;
                        text2 = text.Substring(num);
                    }
                    else
                    {
                        text2 = text.Substring(num, num2 - num);
                    }

                    break;
                case "delete":
                    num2 = text.IndexOf("from");
                    if(num2 < 0)
                    {
                        num2 = num;
                        text2 = text.Substring(num);
                    }
                    else
                    {
                        text2 = text.Substring(num, num2 - num);
                    }

                    num = num2;
                    num2 = text.IndexOf("where");
                    if(num2 < 0)
                    {
                        num2 = num;
                        text3 = text.Substring(num);
                    }
                    else
                    {
                        text3 = text.Substring(num, num2 - num);
                    }

                    break;
                case "if exi":
                    text4 = "update or insert";
                    text3 = text;
                    text2 = text;
                    break;
            }

            num = num2;
            num2 = text.Length - num;
            empty = text.Substring(num, num2);
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            foreach(KeyValuePair<int, _RegistroConsultas> dicRegistroConsulta in _dicRegistroConsultas)
            {
                if(text3.Contains(dicRegistroConsulta.Value._Db) && text3.Contains(dicRegistroConsulta.Value._Tabla) && (string.IsNullOrWhiteSpace(dicRegistroConsulta.Value._Campo) || text2.Contains(dicRegistroConsulta.Value._Campo)) && text4.Contains(dicRegistroConsulta.Value._Accion) && (string.IsNullOrWhiteSpace(dicRegistroConsulta.Value._Condicion) || empty.Contains(dicRegistroConsulta.Value._Condicion)))
                {
                    Escribir_En_Log_Registro_Consulta(text, dicRegistroConsulta.Value._Accion.ToUpper(), dicRegistroConsulta.Value._Tabla.ToUpper());
                }
            }

            SQLChangeConnection(currentAlias);
        }

        private static bool Escribir_En_Log_Registro_Consulta(string tcInstruccionSql, string tcAccion, string tcTabla)
        {
            string tcPila = string.Empty;
            string tcProces = string.Empty;
            int tnLinea = 0;
            string txValor = Convert.ToString(_GetVariable("wc_empresa"));
            string empty = string.Empty;
            StackTrace stackTrace = new StackTrace(fNeedFileInfo: true);
            string empty2 = string.Empty;
            string empty3 = string.Empty;
            int num = 2;
            ObtenerPila(ref tcPila, ref tcProces, ref tnLinea);
            StackFrame frame = stackTrace.GetFrame(num);
            empty2 = frame.GetMethod().Module.Name;
            empty3 = empty2;
            while(empty2.Contains("sage.ew.") && num < stackTrace.FrameCount)
            {
                empty3 = empty2;
                num++;
                frame = stackTrace.GetFrame(num);
                empty2 = ((frame != null) ? frame.GetMethod().Module.Name : string.Empty);
            }

            empty2 = empty3;
            empty = ((!Convert.ToBoolean(_GetVariable("_Cargados_Diccionarios"))) ? Usuario_EW : Convert.ToString(_GetVariable("wc_usuario")));
            tcPila = tcInstruccionSql + Environment.NewLine + Environment.NewLine + tcPila;
            if(tcInstruccionSql.Length > 150)
            {
                tcInstruccionSql = tcInstruccionSql.Substring(0, 150);
            }

            _lRegistro_Consulta_Recursivo = true;
            bool result = SQLExec("INSERT INTO " + SQLDatabase("eurowinsys", "reg_cons") + "  (empresa, tabla, pila, usuario, fecha, adicional, tipodoc, tipo)  VALUES(" + SQLString(txValor) + "," + SQLString(tcTabla) + "," + SQLString(tcPila) + "," + SQLString(empty) + ", getdate(), " + SQLString(tcInstruccionSql) + ", " + SQLString(empty2) + "," + SQLString(tcAccion) + ")");
            _lRegistro_Consulta_Recursivo = false;
            return result;
        }

        public static void Registrar_Error(Exception toEx)
        {
            _ = string.Empty;
            Error_Message_Exception = toEx;
            if(toEx != null)
            {
                Error_Message = SQLString(toEx.Message).Trim();
                NotificarExcepcionParaLosProgramadores(toEx);
                if(Registro_Errores)
                {
                    string currentAlias = CurrentAlias;
                    SQLChangeConnection("eurowin");
                    Escribir_En_Log_Error(Modo_Registro.Registro_Error, toEx, Error_Message);
                    SQLChangeConnection(currentAlias);
                }
            }
        }

        private static void NotificarExcepcionParaLosProgramadores(Exception toEx)
        {
            if(Debugger.IsAttached)
            {
                new frmExcepciones(toEx).ShowDialog();
            }
        }

        private static bool Comprovacions_EurowinSys()
        {
            bool result = false;
            if(_lEurowinSys_Checked)
            {
                return true;
            }

            _lEurowinSys_Checked = true;
            if(Crear_EurowinSys())
            {
                result = Comprobar_Actualiza();
                result = Comprobar_Ewaplica() && result;
                result = Comprobar_File_Index() && result;
                result = Comprobar_File_Indexcd() && result;
                result = Comprobar_Log_Analisis() && result;
                result = Comprobar_Log_Erp() && result;
                result = Comprobar_Log_Error() && result;
                result = Comprobar_Log_Offline() && result;
                result = Comprobar_Reg_Cons() && result;
                result = Comprobar_Terminalseg() && result;
                result = Comprobar_EmpreApi() && result;
                result = Comprobar_UniUsers() && result;
                result = Comprobar_AccesoGrup() && result;
                result = Comprobar_GruposEmp() && result;
                result = Comprobar_Apertura() && result;
            }

            return result;
        }

        private static bool Crear_EurowinSys()
        {
            bool flag = true;
            _ = string.Empty;
            string text = string.Empty;
            if(_SQLExisteBBDD("EUROWINSYS"))
            {
                return true;
            }

            if(!string.IsNullOrWhiteSpace(DbComunes))
            {
                DataTable dtTabla = new DataTable();
                if(SQLExec("SELECT physical_name AS RUTA FROM [" + DbComunes.Trim() + "].SYS.DATABASE_FILES", ref dtTabla) && dtTabla.Rows.Count > 0)
                {
                    text = Convert.ToString(dtTabla.Rows[0]["RUTA"]).Trim();
                    text = System.IO.Path.GetDirectoryName(text) + System.IO.Path.DirectorySeparatorChar;
                }

                flag = SQLExec("CREATE DATABASE [EUROWINSYS] ON  PRIMARY ( NAME = N'EUROWINSYS', FILENAME = N'" + text + "EUROWINSYS.mdf' , FILEGROWTH = 1024KB ),   FILEGROUP [eurowind] ( NAME = N'EUROWINSYS_dat', FILENAME = N'" + text + "EUROWINSYS_dat.ndf' , FILEGROWTH = 1024KB ),   FILEGROUP [eurowinI] ( NAME = N'EUROWINSYS_idx', FILENAME = N'" + text + "EUROWINSYS_Idx.ndf' , FILEGROWTH = 1024KB )  LOG ON  ( NAME = N'EUROWINSYS_log', FILENAME = N'" + text + "EUROWINSYS_log.ldf' , FILEGROWTH = 10%) ALTER DATABASE [EUROWINSYS] SET ANSI_NULL_DEFAULT OFF ALTER DATABASE [EUROWINSYS] SET ANSI_NULLS OFF ALTER DATABASE [EUROWINSYS] SET ANSI_PADDING ON ALTER DATABASE [EUROWINSYS] SET ANSI_WARNINGS OFF ALTER DATABASE [EUROWINSYS] SET ARITHABORT OFF ALTER DATABASE [EUROWINSYS] SET AUTO_CLOSE OFF ALTER DATABASE [EUROWINSYS] COLLATE Modern_Spanish_CI_AI ALTER DATABASE [EUROWINSYS] SET AUTO_CREATE_STATISTICS ON ALTER DATABASE [EUROWINSYS] SET AUTO_SHRINK OFF ALTER DATABASE [EUROWINSYS] SET AUTO_UPDATE_STATISTICS ON ALTER DATABASE [EUROWINSYS] SET CURSOR_CLOSE_ON_COMMIT OFF ALTER DATABASE [EUROWINSYS] SET CURSOR_DEFAULT  GLOBAL ALTER DATABASE [EUROWINSYS] SET CONCAT_NULL_YIELDS_NULL OFF ALTER DATABASE [EUROWINSYS] SET NUMERIC_ROUNDABORT OFF ALTER DATABASE [EUROWINSYS] SET QUOTED_IDENTIFIER OFF ALTER DATABASE [EUROWINSYS] SET RECURSIVE_TRIGGERS OFF ALTER DATABASE [EUROWINSYS] SET AUTO_UPDATE_STATISTICS_ASYNC OFF ALTER DATABASE [EUROWINSYS] SET DATE_CORRELATION_OPTIMIZATION OFF ALTER DATABASE [EUROWINSYS] SET PARAMETERIZATION SIMPLE ALTER DATABASE [EUROWINSYS] SET READ_WRITE ALTER DATABASE [EUROWINSYS] SET RECOVERY SIMPLE ALTER DATABASE [EUROWINSYS] SET MULTI_USER ALTER DATABASE [EUROWINSYS] SET PAGE_VERIFY CHECKSUM IF NOT EXISTS (SELECT name FROM [EUROWINSYS].sys.filegroups WHERE is_default=1 AND name = N'PRIMARY')  ALTER DATABASE [EUROWINSYS] MODIFY FILEGROUP [PRIMARY] DEFAULT ");
                if(flag)
                {
                    flag = SQLExec("ALTER DATABASE [EUROWINSYS] SET PARAMETERIZATION FORCED WITH NO_WAIT");
                }
            }

            return flag;
        }

        private static bool Comprobar_GruposEmp()
        {
            bool flag = false;
            bool flag2 = false;
            bool flag3 = false;
            string empty = string.Empty;
            string text = Convert.ToString(_GetVariable("wc_iniservidor")).Trim();
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            if(SQLExisteTabla("GRUPOSEMP"))
            {
                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "codpripal"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD CODPRIPAL CHAR(4) NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__codpripal]  DEFAULT ''";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                        DataTable dtTabla = new DataTable();
                        string txValor = "";
                        flag = SQLExec("select codigo from [EUROWINSYS].[dbo].[gruposemp] where pripal=" + SQLTrue(), ref dtTabla);
                        if(dtTabla.Rows.Count >= 1)
                        {
                            txValor = Convert.ToString(dtTabla.Rows[0]["codigo"]);
                        }

                        empty = "UPDATE [EUROWINSYS].[dbo].[gruposemp] SET CODPRIPAL=" + SQLString(txValor);
                        flag = flag && SQLExec(empty);
                    }
                    else
                    {
                        SQLRollback();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "guid_id"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD GUID_ID CHAR(50) NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__gruid_id]  DEFAULT (newid())";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "CREATED"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD CREATED DATETIME NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__created]  DEFAULT (getdate())";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "MODIFIED"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD MODIFIED DATETIME NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__modified]  DEFAULT (getdate())";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "CONTACT"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD CONTACT BIT NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__contact]  DEFAULT ((0))";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "FREC_CTC"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD FREC_CTC INT NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__frec_ctc]  DEFAULT ((0))";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "ULT_SYNC"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD ULT_SYNC DATETIME NULL";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "COPIA"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD COPIA BIT NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__copia]  DEFAULT ((0))";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "BCK_CFG"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD BCK_CFG TEXT NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__bck_cfg]  DEFAULT ('')";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "FECHA_CO"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD FECHA_CO BIT NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__fecha_co]  DEFAULT ((0))";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "AUTO_CON"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD AUTO_CON BIT NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__auto_con]  DEFAULT ((0))";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "NOTIFICA"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD NOTIFICA BIT NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__notifica]  DEFAULT ((0))";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "LETRA_CAP"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD LETRA_CAP CHAR(2) NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__letra_cap]  DEFAULT ('')";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "DES_FOTO"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD DES_FOTO BIT NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__des_foto]  DEFAULT ((0))";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "FREC_DAS"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD FREC_DAS INT NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__frec_das]  DEFAULT ((0))";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "EJER_CON"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD EJER_CON CHAR(15) NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__ejer_con]  DEFAULT ((''))";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "NUEVOEJER"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD NUEVOEJER TEXT NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__nuevoejer]  DEFAULT ((''))";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "COPIATODOS"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD COPIATODOS BIT NOT NULL CONSTRAINT [df__eurowinsys__gruposemp__copiatodos]  DEFAULT ((0))";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "IDSAGE50"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD IDSAGE50 CHAR(50) NOT NULL CONSTRAINT  [df__eurowinsys__gruposemp__idsage50]  DEFAULT (newid())";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                if(!SQLExisteCampo("EUROWINSYS", "GRUPOSEMP", "VALIDCHECK"))
                {
                    SQLBegin();
                    empty = " ALTER TABLE [EUROWINSYS].[dbo].[gruposemp]   ADD VALIDCHECK CHAR(64) NOT NULL CONSTRAINT  [df__eurowinsys__gruposemp__validcheck]  DEFAULT ((''))";
                    if(SQLExec(empty))
                    {
                        SQLCommit();
                    }
                }

                DataTable dtTabla2 = new DataTable();
                empty = "SELECT * FROM " + SQLDatabase("EUROWINSYS", "GRUPOSEMP");
                flag = SQLExec(empty, ref dtTabla2);
                if(flag && dtTabla2 != null && dtTabla2.Rows.Count == 0)
                {
                    flag2 = true;
                }
            }
            else
            {
                flag = _Crear_Tabla_GruposEmp();
                flag2 = true;
                flag3 = true;
            }

            if(flag2 && !string.IsNullOrWhiteSpace(text) && File.Exists(System.IO.Path.Combine(text, "gruposemp.xml")))
            {
                _GrupoEmpresa_Xml2Table();
            }

            if(!string.IsNullOrWhiteSpace(DbComunes))
            {
                string text2 = DbComunes.Trim().Substring(4, 4);
                DataTable dtTabla3 = new DataTable();
                SQLExec("select codigo from [EUROWINSYS].[dbo].[gruposemp] where codigo='" + text2 + "'", ref dtTabla3);
                if(dtTabla3.Rows.Count == 0)
                {
                    empty = "INSERT INTO [EUROWINSYS].[dbo].[gruposemp] (CODIGO, NOMBRE, PRIPAL, CODPRIPAL) VALUES (" + SQLString(text2) + ",'GRUPO PRINCIPAL'," + SQLTrue() + "," + SQLString(text2) + ")";
                    flag = flag && SQLExec(empty);
                }
            }

            if(flag3)
            {
                DataTable dtTabla4 = new DataTable();
                SQLExec("SELECT * FROM " + SQLDatabase("EUROWINSYS", "gruposemp"), ref dtTabla4);
                foreach(DataRow row in dtTabla4.Rows)
                {
                    string text3 = "COMU" + row["codigo"].ToString().Trim();
                    string value = "";
                    if(!_oAliasDB.TryGetValue(text3, out value))
                    {
                        _oAliasDB.Add(text3, "[" + text3.ToLower() + "].dbo.");
                    }
                }
            }

            _GrupoEmpresa_Table2Xml();
            SQLChangeConnection(currentAlias);
            return flag;
        }

        private static bool Comprobar_Actualiza()
        {
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            if(SQLExisteTabla("actualiza", "eurowinsys"))
            {
                return true;
            }

            string empty = string.Empty;
            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[actualiza](  [ID] [char](20) NOT NULL,  [FECHA] [smalldatetime] NULL,  [VERSION] [char](20) NOT NULL,  [INCIDENCIA] [text] NULL,  [REVISADO] [bit] NOT NULL,  [APLICA] [int] NOT NULL,  [VISTA] [bit] NOT NULL,  CONSTRAINT [pk__eurowinsys__actualiza__id] PRIMARY KEY CLUSTERED   (  [ID] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind]  ) ON [eurowind] TEXTIMAGE_ON [eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[actualiza] ADD  CONSTRAINT [df__eurowinsys__actualiza__id]  DEFAULT ('') FOR [ID]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[actualiza] ADD  CONSTRAINT [df__eurowinsys__actualiza__fecha]  DEFAULT (getdate()) FOR [FECHA]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[actualiza] ADD  CONSTRAINT [df__eurowinsys__actualiza__version]  DEFAULT ('') FOR [VERSION]";
            bool num4 = num3 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[actualiza] ADD  CONSTRAINT [df__eurowinsys__actualiza__incidencia]  DEFAULT ('') FOR [INCIDENCIA]";
            bool num5 = num4 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[actualiza] ADD  CONSTRAINT [df__eurowinsys__actualiza__revisado]  DEFAULT ((0)) FOR [REVISADO]";
            bool num6 = num5 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[actualiza] ADD  CONSTRAINT [df__eurowinsys__actualiza__aplica]  DEFAULT ((0)) FOR [APLICA]";
            bool num7 = num6 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[actualiza] ADD  CONSTRAINT [df__eurowinsys__actualiza__vista]  DEFAULT ((1)) FOR [VISTA]";
            int num8;
            if(num7)
            {
                num8 = (SQLExec(empty) ? 1 : 0);
                if(num8 != 0)
                {
                    SQLCommit();
                    goto IL_00c2;
                }
            }
            else
            {
                num8 = 0;
            }

            SQLRollback();
            goto IL_00c2;
            IL_00c2:
            SQLChangeConnection(currentAlias);
            return (byte)num8 != 0;
        }

        private static bool Comprobar_Ewaplica()
        {
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            if(SQLExisteTabla("ewaplica", "eurowinsys"))
            {
                return true;
            }

            string empty = string.Empty;
            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[ewaplica](  [CODIGO] [int] NOT NULL,  [COMUNES] [char](8) NOT NULL,  [VISTA] [bit] NOT NULL,  CONSTRAINT [pk__eurowinsys__ewaplica__cod] PRIMARY KEY CLUSTERED   (  [CODIGO] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind] ) ON [eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[ewaplica] ADD  CONSTRAINT [df__eurowinsys__ewaplica__codigo]  DEFAULT ((0)) FOR [CODIGO]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[ewaplica] ADD  CONSTRAINT [df__eurowinsys__ewaplica__comunes]  DEFAULT ('') FOR [COMUNES]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[ewaplica] ADD  CONSTRAINT [df__eurowinsys__ewaplica__vista]  DEFAULT ((1)) FOR [VISTA]";
            int num4;
            if(num3)
            {
                num4 = (SQLExec(empty) ? 1 : 0);
                if(num4 != 0)
                {
                    SQLCommit();
                    goto IL_007e;
                }
            }
            else
            {
                num4 = 0;
            }

            SQLRollback();
            goto IL_007e;
            IL_007e:
            SQLChangeConnection(currentAlias);
            return (byte)num4 != 0;
        }

        private static bool Comprobar_File_Index()
        {
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            if(SQLExisteTabla("file_index", "eurowinsys"))
            {
                return true;
            }

            string empty = string.Empty;
            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[file_index](  [APLICA] [int] NOT NULL,  [PATH] [char](200) NOT NULL,  [ARCHIVO] [char](60) NOT NULL,  [BYTES] [int] NOT NULL,  [ULT_MOD] [datetime] NOT NULL,  [VISTA] [bit] NOT NULL,  CONSTRAINT [pk__eurowinsys__file_index__aplpatarc] PRIMARY KEY CLUSTERED  (  [APLICA] ASC,  [PATH] ASC,  [ARCHIVO] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind]  ) ON [eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[file_index] ADD  CONSTRAINT [df__eurowinsys__file_index__aplica]  DEFAULT ((0)) FOR [APLICA]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[file_index] ADD  CONSTRAINT [df__eurowinsys__file_index__path]  DEFAULT ('') FOR [PATH]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[file_index] ADD  CONSTRAINT [df__eurowinsys__file_index__archivo]  DEFAULT ('') FOR [ARCHIVO]";
            bool num4 = num3 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[file_index] ADD  CONSTRAINT [df__eurowinsys__file_index__bytes]  DEFAULT ((0)) FOR [BYTES]";
            bool num5 = num4 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[file_index] ADD  CONSTRAINT [df__eurowinsys__file_index__ult_mod]  DEFAULT (getdate()) FOR [ULT_MOD]";
            bool num6 = num5 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[file_index] ADD  CONSTRAINT [df__eurowinsys__file_index__vista]  DEFAULT ((1)) FOR [VISTA]";
            int num7;
            if(num6)
            {
                num7 = (SQLExec(empty) ? 1 : 0);
                if(num7 != 0)
                {
                    SQLCommit();
                    goto IL_00b1;
                }
            }
            else
            {
                num7 = 0;
            }

            SQLRollback();
            goto IL_00b1;
            IL_00b1:
            SQLChangeConnection(currentAlias);
            return (byte)num7 != 0;
        }

        private static bool Comprobar_File_Indexcd()
        {
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            if(SQLExisteTabla("file_indexcd", "eurowinsys"))
            {
                return true;
            }

            string empty = string.Empty;
            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[file_indexcd](  [PATH] [char](200) NOT NULL,  [PATH_CD] [char](200) NOT NULL,  [ARCHIVO] [char](60) NOT NULL,  [BYTES] [int] NOT NULL,  [ULT_MOD] [datetime] NOT NULL,  [VISTA] [bit] NOT NULL,  CONSTRAINT [pk__eurowinsys__file_indexcd__patarc] PRIMARY KEY CLUSTERED   (  [PATH] ASC,  [ARCHIVO] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind]  ) ON [eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[file_indexcd] ADD  CONSTRAINT [df__eurowinsys__file_indexcd__path]  DEFAULT ('') FOR [PATH]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[file_indexcd] ADD  CONSTRAINT [df__eurowinsys__file_indexcd__path_cd]  DEFAULT ('') FOR [PATH_CD]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[file_indexcd] ADD  CONSTRAINT [df__eurowinsys__file_indexcd__archivo]  DEFAULT ('') FOR [ARCHIVO]";
            bool num4 = num3 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[file_indexcd] ADD  CONSTRAINT [df__eurowinsys__file_indexcd__bytes]  DEFAULT ((0)) FOR [BYTES]";
            bool num5 = num4 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[file_indexcd] ADD  CONSTRAINT [df__eurowinsys__file_indexcd__ult_mod]  DEFAULT (getdate()) FOR [ULT_MOD]";
            bool num6 = num5 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[file_indexcd] ADD  CONSTRAINT [df__eurowinsys__file_indexcd__vista]  DEFAULT ((1)) FOR [VISTA]";
            int num7;
            if(num6)
            {
                num7 = (SQLExec(empty) ? 1 : 0);
                if(num7 != 0)
                {
                    SQLCommit();
                    goto IL_00b1;
                }
            }
            else
            {
                num7 = 0;
            }

            SQLRollback();
            goto IL_00b1;
            IL_00b1:
            SQLChangeConnection(currentAlias);
            return (byte)num7 != 0;
        }

        private static bool Comprobar_Log_Analisis()
        {
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            string empty = string.Empty;
            bool flag = false;
            if(!SQLExisteTabla("log_analisis", "eurowinsys"))
            {
                SQLBegin();
                empty = "CREATE TABLE [EUROWINSYS].[dbo].[log_analisis]( [APLICA] [int] NOT NULL, [TERMINAL] [char](15) NOT NULL, [USUARI] [char](15) NOT NULL, [TEMPSHORA] [datetime] NULL, [TIPO] [char](3) NOT NULL, [LIBRERIA] [char](100) NOT NULL, [FICHERO] [char](25) NOT NULL, [TEMPSRELATIU] [numeric](20, 6) NOT NULL, [TEMPSACUMULAT] [numeric](20, 6) NOT NULL, [MISSATGE] [char](220) NOT NULL, [CONSULTA] [text] NOT NULL, [PILA] [text] NOT NULL, [HILO] [bit] NOT NULL, [INFOHILO] [text] NOT NULL, [GUID] [char] (50) NOT NULL, [VISTA] [bit] NOT NULL,  CONSTRAINT [pk__eurowinsys__log_analisis__guid] PRIMARY KEY CLUSTERED   (  [GUID] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind] ) ON [eurowind] TEXTIMAGE_ON [eurowind] ";
                flag = SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__aplica]  DEFAULT ((0)) FOR [APLICA] ";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__terminal]  DEFAULT ('') FOR [TERMINAL]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__usuari]  DEFAULT ('') FOR [USUARI]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__tempshora]  DEFAULT (getdate()) FOR [TEMPSHORA]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__taula]  DEFAULT ('') FOR [TIPO]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__libreria]  DEFAULT ('') FOR [LIBRERIA]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__fichero]  DEFAULT ('') FOR [FICHERO]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__tempsrelatiu]  DEFAULT ((0.000000)) FOR [TEMPSRELATIU]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__tempsacumulat]  DEFAULT ((0.000000)) FOR [TEMPSACUMULAT]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__missatge]  DEFAULT ('') FOR [MISSATGE]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__consulta]  DEFAULT ('') FOR [CONSULTA]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__pila]  DEFAULT ('') FOR [PILA]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__hilo]  DEFAULT ((0)) FOR [HILO]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__infohilo]  DEFAULT ('') FOR [INFOHILO]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__guid]  DEFAULT (newid()) FOR [GUID]";
                flag = flag && SQLExec(empty);
                empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD  CONSTRAINT [df__eurowinsys__log_analisis__vista]  DEFAULT ((0)) FOR [VISTA]";
                flag = flag && SQLExec(empty);
                if(flag)
                {
                    SQLCommit();
                    if(flag)
                    {
                        empty = "CREATE NONCLUSTERED INDEX [TIPO] ON [EUROWINSYS].[dbo].[log_analisis] ( [TIPO] ASC )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  ON [eurowini]";
                        flag = flag && SQLExec(empty);
                    }

                    if(flag)
                    {
                        empty = "CREATE NONCLUSTERED INDEX [TEMPSHORA] ON [EUROWINSYS].[dbo].[log_analisis] ( [TEMPSHORA] ASC )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  ON [eurowini]";
                        flag = flag && SQLExec(empty);
                    }
                }
                else
                {
                    SQLRollback();
                }
            }
            else if(!SQLExisteCampo("EUROWINSYS", "log_analisis", "guid"))
            {
                SQLBegin();
                empty = " ALTER TABLE [EUROWINSYS].[dbo].[log_analisis]   ADD GUID CHAR(50) NOT NULL CONSTRAINT [df__eurowinsys__log_analisis__guid]  DEFAULT (newid()) ";
                flag = SQLExec(empty);
                empty = " ALTER TABLE [EUROWINSYS].[dbo].[log_analisis] ADD CONSTRAINT [pk__eurowinsys__log_analisis__guid]  PRIMARY KEY CLUSTERED ( [guid] ASC)  WITH ( STATISTICS_NORECOMPUTE = OFF,  IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  ON [eurowind]; ";
                flag = flag && SQLExec(empty);
                SQLCommit();
            }

            SQLChangeConnection(currentAlias);
            return flag;
        }

        private static bool Comprobar_Log_Erp()
        {
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            if(SQLExisteTabla("log_erp", "eurowinsys"))
            {
                return true;
            }

            string empty = string.Empty;
            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[log_erp](  [IDLOG] [char](10) NOT NULL,  [APLICA] [int] NOT NULL,  [TERMINAL] [char](20) NOT NULL,  [USUARI] [char](15) NOT NULL,  [TEMPSHORA] [datetime] NULL,  [TIPO] [char](20) NOT NULL,  [METODO] [char](50) NOT NULL,  [OBSERVA] [text] NOT NULL,  [AUTOMATICO] [bit] NOT NULL,  [VISTA] [bit] NOT NULL,  CONSTRAINT [pk__eurowinsys__log_erp__idl] PRIMARY KEY CLUSTERED  (  [IDLOG] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind]  ) ON [eurowind] TEXTIMAGE_ON [eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_erp] ADD  CONSTRAINT [df__eurowinsys__log_erp__idlog]  DEFAULT ('') FOR [IDLOG]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_erp] ADD  CONSTRAINT [df__eurowinsys__log_erp__aplica]  DEFAULT ((0)) FOR [APLICA]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_erp] ADD  CONSTRAINT [df__eurowinsys__log_erp__terminal]  DEFAULT ('') FOR [TERMINAL]";
            bool num4 = num3 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_erp] ADD  CONSTRAINT [df__eurowinsys__log_erp__usuari]  DEFAULT ('') FOR [USUARI]";
            bool num5 = num4 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_erp] ADD  CONSTRAINT [df__eurowinsys__log_erp__tempshora]  DEFAULT (getdate()) FOR [TEMPSHORA]";
            bool num6 = num5 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_erp] ADD  CONSTRAINT [df__eurowinsys__log_erp__tipo]  DEFAULT ('') FOR [TIPO]";
            bool num7 = num6 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_erp] ADD  CONSTRAINT [df__eurowinsys__log_erp__metodo]  DEFAULT ('') FOR [METODO]";
            bool num8 = num7 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_erp] ADD  CONSTRAINT [df__eurowinsys__log_erp__observa]  DEFAULT ('') FOR [OBSERVA]";
            bool num9 = num8 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_erp] ADD  CONSTRAINT [df__eurowinsys__log_erp__automatico]  DEFAULT ((0)) FOR [AUTOMATICO]";
            bool num10 = num9 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_erp] ADD  CONSTRAINT [df__eurowinsys__log_erp__vista]  DEFAULT ((1)) FOR [VISTA]";
            int num11;
            if(num10)
            {
                num11 = (SQLExec(empty) ? 1 : 0);
                if(num11 != 0)
                {
                    SQLCommit();
                    goto IL_00f5;
                }
            }
            else
            {
                num11 = 0;
            }

            SQLRollback();
            goto IL_00f5;
            IL_00f5:
            SQLChangeConnection(currentAlias);
            return (byte)num11 != 0;
        }

        private static bool Comprobar_Log_Error()
        {
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            if(SQLExisteTabla("log_error", "eurowinsys"))
            {
                return true;
            }

            string empty = string.Empty;
            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[log_error](  [APLICA] [int] NOT NULL,  [TERMINAL] [char](15) NOT NULL,  [USUARI] [char](15) NOT NULL,  [TEMPSHORA] [datetime] NULL,  [FORMULARI] [char](25) NOT NULL,  [TAULA] [char](10) NOT NULL,  [PROCES] [char](50) NOT NULL,  [LINEA] [numeric](10, 0) NOT NULL,  [CODI_ERROR] [int] NOT NULL,  [MISSATGE] [char](220) NOT NULL,  [PILA] [text] NULL, [GUID] [char] (50) NOT NULL,  [VISTA] [bit] NOT NULL  CONSTRAINT [pk__eurowinsys__log_error__guid] PRIMARY KEY CLUSTERED   (  [GUID] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind] ) ON [eurowind] TEXTIMAGE_ON [eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_error] ADD  CONSTRAINT [df__eurowinsys__log_error__aplica]  DEFAULT ((0)) FOR [APLICA]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_error] ADD  CONSTRAINT [df__eurowinsys__log_error__terminal]  DEFAULT ('') FOR [TERMINAL]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_error] ADD  CONSTRAINT [df__eurowinsys__log_error__usuari]  DEFAULT ('') FOR [USUARI]";
            bool num4 = num3 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_error] ADD  CONSTRAINT [df__eurowinsys__log_error__tempshora]  DEFAULT (getdate()) FOR [TEMPSHORA]";
            bool num5 = num4 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_error] ADD  CONSTRAINT [df__eurowinsys__log_error__formulari]  DEFAULT ('') FOR [FORMULARI]";
            bool num6 = num5 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_error] ADD  CONSTRAINT [df__eurowinsys__log_error__taula]  DEFAULT ('') FOR [TAULA]";
            bool num7 = num6 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_error] ADD  CONSTRAINT [df__eurowinsys__log_error__proces]  DEFAULT ('') FOR [PROCES]";
            bool num8 = num7 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_error] ADD  CONSTRAINT [df__eurowinsys__log_error__linea]  DEFAULT ((0)) FOR [LINEA]";
            bool num9 = num8 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_error] ADD  CONSTRAINT [df__eurowinsys__log_error__codi_error]  DEFAULT ((0)) FOR [CODI_ERROR]";
            bool num10 = num9 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_error] ADD  CONSTRAINT [df__eurowinsys__log_error__missatge]  DEFAULT ('') FOR [MISSATGE]";
            bool num11 = num10 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_error] ADD  CONSTRAINT [df__eurowinsys__log_error__pila]  DEFAULT ('') FOR [PILA]";
            bool num12 = num11 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_error] ADD  CONSTRAINT [df__eurowinsys__log_error__guid]  DEFAULT (newid()) FOR [GUID]";
            bool num13 = num12 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_error] ADD  CONSTRAINT [df__eurowinsys__log_error__vista]  DEFAULT ((0)) FOR [VISTA]";
            int num14;
            if(num13)
            {
                num14 = (SQLExec(empty) ? 1 : 0);
                if(num14 != 0)
                {
                    SQLCommit();
                    goto IL_0128;
                }
            }
            else
            {
                num14 = 0;
            }

            SQLRollback();
            goto IL_0128;
            IL_0128:
            SQLChangeConnection(currentAlias);
            return (byte)num14 != 0;
        }

        private static bool Comprobar_Log_Offline()
        {
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            if(SQLExisteTabla("log_offline", "eurowinsys"))
            {
                return true;
            }

            string empty = string.Empty;
            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[log_offline](  [IDLOG] [char](10) NOT NULL,  [APLICA] [int] NOT NULL,  [TERMINAL] [char](20) NOT NULL,  [USUARI] [char](15) NOT NULL,  [TEMPSHORA] [datetime] NULL,  [TIPO] [char](20) NOT NULL,  [OPERACIO] [char](50) NOT NULL,  [OBSERVA] [text] NOT NULL,  [SINCRONIZA] [int] NOT NULL,  [TIPOLOG] [int] NOT NULL,  [VISTA] [bit] NOT NULL,  [idoffline] [char](2) NOT NULL,  [bloqueo] [char](10) NOT NULL,  [bdtabla] [char](30) NOT NULL,  [temps] [numeric](15, 2) NOT NULL,  CONSTRAINT [pk__eurowinsys__log_offline__idl] PRIMARY KEY CLUSTERED   (  [IDLOG] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind]  ) ON [eurowind] TEXTIMAGE_ON [eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__idlog]  DEFAULT ('') FOR [IDLOG]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__aplica]  DEFAULT ((0)) FOR [APLICA]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__terminal]  DEFAULT ('') FOR [TERMINAL]";
            bool num4 = num3 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__usuari]  DEFAULT ('') FOR [USUARI]";
            bool num5 = num4 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__tempshora]  DEFAULT (getdate()) FOR [TEMPSHORA]";
            bool num6 = num5 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__tipo]  DEFAULT ('') FOR [TIPO]";
            bool num7 = num6 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__operacio]  DEFAULT ('') FOR [OPERACIO]";
            bool num8 = num7 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__observa]  DEFAULT ('') FOR [OBSERVA]";
            bool num9 = num8 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__sincroniza]  DEFAULT ((0)) FOR [SINCRONIZA]";
            bool num10 = num9 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__tipolog]  DEFAULT ((0)) FOR [TIPOLOG]";
            bool num11 = num10 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__vista]  DEFAULT ((1)) FOR [VISTA]";
            bool num12 = num11 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__idoffline]  DEFAULT ('') FOR [idoffline]";
            bool num13 = num12 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__bloqueo]  DEFAULT ('') FOR [bloqueo]";
            bool num14 = num13 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__bdtabla]  DEFAULT ('') FOR [bdtabla]";
            bool num15 = num14 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[log_offline] ADD  CONSTRAINT [df__eurowinsys__log_offline__temps]  DEFAULT ((0.00)) FOR [temps]";
            int num16;
            if(num15)
            {
                num16 = (SQLExec(empty) ? 1 : 0);
                if(num16 != 0)
                {
                    SQLCommit();
                    goto IL_014a;
                }
            }
            else
            {
                num16 = 0;
            }

            SQLRollback();
            goto IL_014a;
            IL_014a:
            SQLChangeConnection(currentAlias);
            return (byte)num16 != 0;
        }

        private static bool Comprobar_Reg_Cons()
        {
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            if(SQLExisteTabla("reg_cons", "eurowinsys"))
            {
                return true;
            }

            string empty = string.Empty;
            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[reg_cons](  [EMPRESA] [char](2) NOT NULL,  [TABLA] [char](12) NOT NULL,  [TIPODOC] [char](20) NOT NULL,  [NUMERO] [char](10) NOT NULL,  [LETRA] [char](2) NOT NULL,  [LINEA] [numeric](15, 0) NOT NULL,  [ADICIONAL] [char](150) NOT NULL,  [PILA] [text] NULL,  [TIPO] [char](20) NOT NULL,  [USUARIO] [char](25) NOT NULL,  [FECHA] [datetime] NOT NULL,  [VISTA] [bit] NOT NULL  ) ON [eurowind] TEXTIMAGE_ON [eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[reg_cons] ADD  CONSTRAINT [df__eurowinsys__reg_cons__empresa]  DEFAULT ('') FOR [EMPRESA]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[reg_cons] ADD  CONSTRAINT [df__eurowinsys__reg_cons__tabla]  DEFAULT ('') FOR [TABLA]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[reg_cons] ADD  CONSTRAINT [df__eurowinsys__reg_cons__tipodoc]  DEFAULT ('') FOR [TIPODOC]";
            bool num4 = num3 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[reg_cons] ADD  CONSTRAINT [df__eurowinsys__reg_cons__numero]  DEFAULT ('') FOR [NUMERO]";
            bool num5 = num4 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[reg_cons] ADD  CONSTRAINT [df__eurowinsys__reg_cons__letra]  DEFAULT ('') FOR [LETRA]";
            bool num6 = num5 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[reg_cons] ADD  CONSTRAINT [df__eurowinsys__reg_cons__linea]  DEFAULT ((0)) FOR [LINEA]";
            bool num7 = num6 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[reg_cons] ADD  CONSTRAINT [df__eurowinsys__reg_cons__adicional]  DEFAULT ('') FOR [ADICIONAL]";
            bool num8 = num7 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[reg_cons] ADD  CONSTRAINT [df__eurowinsys__reg_cons__pila]  DEFAULT ('') FOR [PILA]";
            bool num9 = num8 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[reg_cons] ADD  CONSTRAINT [df__eurowinsys__reg_cons__tipo]  DEFAULT ('') FOR [TIPO]";
            bool num10 = num9 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[reg_cons] ADD  CONSTRAINT [df__eurowinsys__reg_cons__usuario]  DEFAULT ('') FOR [USUARIO]";
            bool num11 = num10 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[reg_cons] ADD  CONSTRAINT [df__eurowinsys__reg_cons__fecha]  DEFAULT (getdate()) FOR [FECHA]";
            bool num12 = num11 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[reg_cons] ADD  CONSTRAINT [df__eurowinsys__reg_cons__vista]  DEFAULT ((1)) FOR [VISTA]";
            int num13;
            if(num12)
            {
                num13 = (SQLExec(empty) ? 1 : 0);
                if(num13 != 0)
                {
                    SQLCommit();
                    goto IL_0117;
                }
            }
            else
            {
                num13 = 0;
            }

            SQLRollback();
            goto IL_0117;
            IL_0117:
            SQLChangeConnection(currentAlias);
            return (byte)num13 != 0;
        }

        private static bool Comprobar_Terminalseg()
        {
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            if(SQLExisteTabla("terminalseg", "eurowinsys"))
            {
                return true;
            }

            string empty = string.Empty;
            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[terminalseg](  [APLICA] [int] NOT NULL,  [DBOFFLINE] [char](8) NOT NULL,  [VISTA] [bit] NOT NULL,  CONSTRAINT [pk__eurowinsys__terminalseg__apl] PRIMARY KEY CLUSTERED   (  [APLICA] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind]  ) ON [eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[terminalseg] ADD  CONSTRAINT [df__eurowinsys__terminalseg__aplica]  DEFAULT ((0)) FOR [APLICA]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[terminalseg] ADD  CONSTRAINT [df__eurowinsys__terminalseg__dboffline]  DEFAULT ('') FOR [DBOFFLINE]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[terminalseg] ADD  CONSTRAINT [df__eurowinsys__terminalseg__vista]  DEFAULT ((1)) FOR [VISTA]";
            int num4;
            if(num3)
            {
                num4 = (SQLExec(empty) ? 1 : 0);
                if(num4 != 0)
                {
                    SQLCommit();
                    goto IL_007e;
                }
            }
            else
            {
                num4 = 0;
            }

            SQLRollback();
            goto IL_007e;
            IL_007e:
            SQLChangeConnection(currentAlias);
            return (byte)num4 != 0;
        }

        private static bool Comprobar_EmpreApi()
        {
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            if(SQLExisteTabla("empre_api", "eurowinsys"))
            {
                return true;
            }

            string empty = string.Empty;
            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[empre_api](  [APLICA] [int] NOT NULL,  [EJERCICIO] [char](4) NOT NULL,  [GRUPO] [char](4) NOT NULL,  [ORIGEN] [char](2) NOT NULL,  [DESTINO] [char](4) NOT NULL,  [DCONTA] [char](2) NOT NULL,  [BORRADA] [bit] NOT NULL,  [VISTA] [bit] NOT NULL,  CONSTRAINT [pk__eurowinsys__empre_api__apl] PRIMARY KEY CLUSTERED   (  [APLICA] ASC,  [EJERCICIO] ASC,  [GRUPO] ASC,  [ORIGEN] ASC,  [DESTINO] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind]  ) ON [eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[empre_api] ADD  CONSTRAINT [df__eurowinsys__empre_api__aplica]  DEFAULT ((0)) FOR [APLICA]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[empre_api] ADD  CONSTRAINT [df__eurowinsys__empre_api__ejercicio]  DEFAULT ('') FOR [EJERCICIO]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[empre_api] ADD  CONSTRAINT [df__eurowinsys__empre_api__grupo]  DEFAULT ('') FOR [GRUPO]";
            bool num4 = num3 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[empre_api] ADD  CONSTRAINT [df__eurowinsys__empre_api__origen]  DEFAULT ('') FOR [ORIGEN]";
            bool num5 = num4 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[empre_api] ADD  CONSTRAINT [df__eurowinsys__empre_api__destino]  DEFAULT ('') FOR [DESTINO]";
            bool num6 = num5 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[empre_api] ADD  CONSTRAINT [df__eurowinsys__empre_api__dconta]  DEFAULT ('') FOR [DCONTA]";
            bool num7 = num6 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[empre_api] ADD  CONSTRAINT [df__eurowinsys__empre_api__borrada]  DEFAULT ((0)) FOR [BORRADA]";
            bool num8 = num7 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[empre_api] ADD  CONSTRAINT [df__eurowinsys__empre_api__vista]  DEFAULT ((1)) FOR [VISTA]";
            int num9;
            if(num8)
            {
                num9 = (SQLExec(empty) ? 1 : 0);
                if(num9 != 0)
                {
                    SQLCommit();
                    goto IL_00d3;
                }
            }
            else
            {
                num9 = 0;
            }

            SQLRollback();
            goto IL_00d3;
            IL_00d3:
            SQLChangeConnection(currentAlias);
            return (byte)num9 != 0;
        }

        private static bool Comprobar_UniUsers()
        {
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            if(SQLExisteTabla("uniusers", "eurowinsys"))
            {
                return true;
            }

            string empty = string.Empty;
            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[uniusers](  [IDSAGE50][char](50) COLLATE Modern_Spanish_CS_AI NOT NULL,  [IDUSUARIO] [char](50) COLLATE Modern_Spanish_CS_AI NOT NULL,  [IDCLOUDID] [text] COLLATE Modern_Spanish_CS_AI NOT NULL,  [EMAIL] [char](150) COLLATE Modern_Spanish_CS_AI NOT NULL,  [ROL] [int] NOT NULL,  [GUID_ID][char](50) COLLATE Modern_Spanish_CS_AI NOT NULL,  [CREATED] [datetime] NOT NULL,  [MODIFIED] [datetime] NOT NULL  CONSTRAINT[pk__eurowinsys__uniusers__idsidu] PRIMARY KEY CLUSTERED  (  [IDSAGE50] ASC,  [IDUSUARIO] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind]  ) ON[eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[uniusers] ADD  CONSTRAINT [df__eurowinsys__uniusers__idsage50]  DEFAULT ('') FOR [IDSAGE50]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[uniusers] ADD  CONSTRAINT [df__eurowinsys__uniusers__idusuario]  DEFAULT ('') FOR [IDUSUARIO]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[uniusers] ADD  CONSTRAINT [df__eurowinsys__uniusers__idcloudid]  DEFAULT ('') FOR [IDCLOUDID]";
            bool num4 = num3 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[uniusers] ADD  CONSTRAINT [df__eurowinsys__uniusers__email]  DEFAULT ('') FOR [EMAIL]";
            bool num5 = num4 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[uniusers] ADD  CONSTRAINT [df__eurowinsys__uniusers__rol]  DEFAULT ((0)) FOR [ROL]";
            bool num6 = num5 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[uniusers] ADD CONSTRAINT [df__eurowinsys__uniusers__guid_id]  DEFAULT(newid()) FOR [GUID_ID]";
            bool num7 = num6 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[uniusers] ADD CONSTRAINT [df__eurowinsys__uniusers__created]  DEFAULT(getdate()) FOR [CREATED]";
            bool num8 = num7 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[uniusers] ADD CONSTRAINT [df__eurowinsys__uniusers__modified]  DEFAULT(getdate()) FOR [MODIFIED]";
            int num9;
            if(num8)
            {
                num9 = (SQLExec(empty) ? 1 : 0);
                if(num9 != 0)
                {
                    SQLCommit();
                    goto IL_00d3;
                }
            }
            else
            {
                num9 = 0;
            }

            SQLRollback();
            goto IL_00d3;
            IL_00d3:
            SQLChangeConnection(currentAlias);
            return (byte)num9 != 0;
        }

        private static bool Comprobar_AccesoGrup()
        {
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            if(SQLExisteTabla("accesogrup", "eurowinsys"))
            {
                return true;
            }

            string empty = string.Empty;
            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[accesogrup](  [IDSAGE50][char](50) COLLATE Modern_Spanish_CS_AI NOT NULL,  [GRUPO] [char](4) COLLATE Modern_Spanish_CS_AI NOT NULL,  [MODULO] [char](20) COLLATE Modern_Spanish_CS_AI NOT NULL,  [USUARIO] [char](15) COLLATE Modern_Spanish_CS_AI NOT NULL,  [NIVEL] [int] NOT NULL,  [GUID_ID][char](50) COLLATE Modern_Spanish_CS_AI NOT NULL,  [CREATED] [datetime] NOT NULL,  [MODIFIED] [datetime] NOT NULL  CONSTRAINT[pk__eurowinsys__accesogrup__idsage] PRIMARY KEY CLUSTERED  (  [IDSAGE50] ASC,  [GRUPO] ASC,  [MODULO] ASC,  [USUARIO] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind]  ) ON[eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[accesogrup] ADD  CONSTRAINT [df__eurowinsys__accesogrup__idsage50]  DEFAULT ('') FOR [IDSAGE50]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[accesogrup] ADD  CONSTRAINT [df__eurowinsys__accesogrup__grupo]  DEFAULT ('') FOR [GRUPO]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[accesogrup] ADD  CONSTRAINT [df__eurowinsys__accesogrup__modulo]  DEFAULT ('') FOR [MODULO]";
            bool num4 = num3 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[accesogrup] ADD  CONSTRAINT [df__eurowinsys__accesogrup__usuario]  DEFAULT ('') FOR [USUARIO]";
            bool num5 = num4 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[accesogrup] ADD  CONSTRAINT [df__eurowinsys__accesogrup__nivel]  DEFAULT ((0)) FOR [NIVEL]";
            bool num6 = num5 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[accesogrup] ADD CONSTRAINT [df__eurowinsys__accesogrup__guid_id]  DEFAULT(newid()) FOR [GUID_ID]";
            bool num7 = num6 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[accesogrup] ADD CONSTRAINT [df__eurowinsys__accesogrup__created]  DEFAULT(getdate()) FOR [CREATED]";
            bool num8 = num7 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[accesogrup] ADD CONSTRAINT [df__eurowinsys__accesogrup__modified]  DEFAULT(getdate()) FOR [MODIFIED]";
            int num9;
            if(num8)
            {
                num9 = (SQLExec(empty) ? 1 : 0);
                if(num9 != 0)
                {
                    SQLCommit();
                    goto IL_00d3;
                }
            }
            else
            {
                num9 = 0;
            }

            SQLRollback();
            goto IL_00d3;
            IL_00d3:
            SQLChangeConnection(currentAlias);
            return (byte)num9 != 0;
        }

        private static bool Comprobar_Apertura()
        {
            string empty = string.Empty;
            bool flag = false;
            string currentAlias = CurrentAlias;
            SQLChangeConnection("eurowin");
            string text = SQLCampoCollation("eurowinsys", "apertura", "tabla");
            if(!string.IsNullOrWhiteSpace(text) && text == "modern_spanish_ci_ai")
            {
                empty = "drop table " + SQLDatabase("EUROWINSYS", "apertura");
                flag = SQLExec(empty);
            }

            if(!flag && SQLExisteTabla("apertura", "eurowinsys"))
            {
                return true;
            }

            SQLBegin();
            empty = " CREATE TABLE [EUROWINSYS].[dbo].[apertura](  [COMUNES] [char](8) COLLATE Modern_Spanish_CS_AI NOT NULL,  [FECHAHORA] [smalldatetime] NULL,  [ORIGEN] [char](4) COLLATE Modern_Spanish_CS_AI NOT NULL,  [DESTINO] [char](4) COLLATE Modern_Spanish_CS_AI NOT NULL,  [TABLA] [char](20) COLLATE Modern_Spanish_CS_AI NOT NULL,  [CLAVE] [char](100) COLLATE Modern_Spanish_CS_AI NOT NULL,  [VISTA] [bit] NOT NULL,  CONSTRAINT [pk__eurowinsys__apertura__comoritab] PRIMARY KEY CLUSTERED   (  [COMUNES] ASC,  [ORIGEN] ASC,  [TABLA] ASC,  [CLAVE] ASC  )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [eurowind]  ) ON [eurowind] ";
            bool num = SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[apertura] ADD  CONSTRAINT [df__eurowinsys__apertura__comunes]  DEFAULT ('') FOR [COMUNES]";
            bool num2 = num && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[apertura] ADD  CONSTRAINT [df__eurowinsys__apertura__fechahora]  DEFAULT (getdate()) FOR [FECHAHORA]";
            bool num3 = num2 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[apertura] ADD  CONSTRAINT [df__eurowinsys__apertura__origen]  DEFAULT ('') FOR [ORIGEN]";
            bool num4 = num3 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[apertura] ADD  CONSTRAINT [df__eurowinsys__apertura__destino]  DEFAULT ('') FOR [DESTINO]";
            bool num5 = num4 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[apertura] ADD  CONSTRAINT [df__eurowinsys__apertura__tabla]  DEFAULT ('') FOR [TABLA]";
            bool num6 = num5 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[apertura] ADD  CONSTRAINT [df__eurowinsys__apertura__clave]  DEFAULT ('') FOR [CLAVE]";
            bool num7 = num6 && SQLExec(empty);
            empty = "ALTER TABLE [EUROWINSYS].[dbo].[apertura] ADD  CONSTRAINT [df__eurowinsys__apertura__vista]  DEFAULT ((1)) FOR [VISTA]";
            int num8;
            if(num7)
            {
                num8 = (SQLExec(empty) ? 1 : 0);
                if(num8 != 0)
                {
                    SQLCommit();
                    goto IL_0112;
                }
            }
            else
            {
                num8 = 0;
            }

            SQLRollback();
            goto IL_0112;
            IL_0112:
            if(num8 != 0)
            {
                _DBsInformationSchema("EUROWINSYS")._AgregarTabla("APERTURA");
            }

            SQLChangeConnection(currentAlias);
            return (byte)num8 != 0;
        }

        public static _TableInformationSchema _TablesInformationSchema(string tcDataBase, string tcTable)
        {
            string key = tcDataBase.Trim().ToUpper() + tcTable.Trim().ToUpper();
            if(!_dicCacheSchema.ContainsKey(key))
            {
                _TableInformationSchema value = new _TableInformationSchema(tcDataBase, tcTable);
                _dicCacheSchema[key] = value;
            }

            return _dicCacheSchema[key];
        }

        public static void PreloadSchemas()
        {
            List<string> source = new List<string>
        {
            "TECNICOS", "ZONAS", "OBRA", "COMPRAS", "TALLAS", "COLORES", "GRUPTALL", "GRUPCOLO", "CFGBABEL", "COMPRCNF",
            "OTROSCFG", "SOCIAL365", "TERMINAL", "USUARIOS", "CODCOM"
        };
            List<string> obj = new List<string>
        {
            "CFGFACT", "CODIGOS", "CONTACNF", "CONTADOR", "COTIZA", "EMPRESA", "FACTUCNF", "FLAGS", "MODCONFI", "MONEDA",
            "NIVEL1", "NIVEL2", "NIVEL3", "NIVEL4", "FAMILIAS", "MARCAS", "TARIFAS", "SUBFAM", "RUTAS", "VENDEDOR",
            "ARTICULO", "CUENTAS", "BARRAS", "DEFPRES", "CLIENTES", "PROVEED", "SECUNDAR", "ACTIVI", "ALMACEN", "FPAG",
            "IDI_ART", "C_ALBCOM"
        };
            StringBuilder lcQueryCreacionEsquemas = new StringBuilder();
            lcQueryCreacionEsquemas.Append(preloadSchemas_ObtenerQueryCrearSchemaData("COMUNES", source.FirstOrDefault()));
            source.Skip(1).ToList().ForEach(delegate (string lcTabla) {
                lcQueryCreacionEsquemas.Append(preloadSchemas_ObtenerQueryCrearSchemaData("COMUNES", lcTabla, " UNION ALL "));
            });
            obj.ForEach(delegate (string lcTabla) {
                lcQueryCreacionEsquemas.Append(preloadSchemas_ObtenerQueryCrearSchemaData("2024XP", lcTabla, " UNION ALL "));
            });
            preloadSchemas_TratarResultados(preloadSchemas_GerationData(lcQueryCreacionEsquemas.ToString()));
        }

        private static string preloadSchemas_ObtenerQueryCrearSchemaData(string tcDatabase, string tcTabla, string tcConector = "")
        {
            string result = string.Empty;
            string value = string.Empty;
            tcDatabase = tcDatabase.Trim().ToUpper();
            tcTabla = tcTabla.Trim().ToUpper();
            string text = tcDatabase + tcTabla;
            if(!_dicCacheSchema.ContainsKey(text))
            {
                _oAliasDB.TryGetValue(tcDatabase.ToUpper(), out value);
                value = value.Replace(".dbo.", ".INFORMATION_SCHEMA.");
                result = tcConector + "SELECT " + SQLString(tcDatabase) + " AS BBDD, " + SQLString(tcTabla) + " AS DATATABLE, " + SQLString(text) + " as CLAVEDICCIONARIO, column_name,data_type,is_nullable,character_maximum_length,column_default,numeric_precision,numeric_scale,ordinal_position  FROM " + value + "COLUMNS  WHERE table_name = " + SQLString(tcTabla);
            }

            return result;
        }

        private static void preloadSchemas_TratarResultados(DataTable tdtDatos)
        {
            List<DataRow> dataRows = tdtDatos.AsEnumerable().Cast<DataRow>().ToList();
            dataRows.Select((DataRow ldr) => ldr.Field<string>("CLAVEDICCIONARIO")).Distinct().ToList()
                .ForEach(delegate (string lcClave) {
                    preloadSchemas_GenerateSchemaFromDataRows(lcClave, dataRows.Where((DataRow ldr) => ldr.Field<string>("CLAVEDICCIONARIO") == lcClave).ToList());
                });
        }

        private static DataTable preloadSchemas_GerationData(string tcQuery)
        {
            bool flag = false;
            DataTable dtTabla = new DataTable();
            _ = string.Empty;
            if(string.IsNullOrEmpty(tcQuery))
            {
                return dtTabla;
            }

            try
            {
                SqlConnection sqlConnection = new SqlConnection(Conexion);
                if(sqlConnection.State == ConnectionState.Open)
                {
                    flag = true;
                }
                else
                {
                    _SQLOpen(sqlConnection);
                    flag = false;
                }

                SQLExec(tcQuery, ref dtTabla);
                if(!flag)
                {
                    sqlConnection.Close();
                }
            }
            catch(Exception toEx)
            {
                Registrar_Error(toEx);
            }

            return dtTabla;
        }

        private static void preloadSchemas_GenerateSchemaFromDataRows(string tcClave, List<DataRow> tlisRowSchemaData)
        {
            if(_dicCacheSchema.ContainsKey(tcClave))
            {
                return;
            }

            _TableInformationSchema tableInformationSchema = new _TableInformationSchema();
            DataRow dataRow = tlisRowSchemaData.First();
            tableInformationSchema._DataBase = Convert.ToString(dataRow["BBDD"]).TrimEnd();
            tableInformationSchema._Table = Convert.ToString(dataRow["DATATABLE"]).TrimEnd();
            DataTable dataTable = preloadSchemas_ObtenerModeloDataTable(dataRow);
            foreach(DataRow tlisRowSchemaDatum in tlisRowSchemaData)
            {
                DataRow dataRow2 = dataTable.NewRow();
                dataRow2["column_name"] = tlisRowSchemaDatum["column_name"];
                dataRow2["data_type"] = tlisRowSchemaDatum["data_type"];
                dataRow2["is_nullable"] = tlisRowSchemaDatum["is_nullable"];
                dataRow2["character_maximum_length"] = tlisRowSchemaDatum["character_maximum_length"];
                dataRow2["column_default"] = tlisRowSchemaDatum["column_default"];
                dataRow2["numeric_precision"] = tlisRowSchemaDatum["numeric_precision"];
                dataRow2["numeric_scale"] = tlisRowSchemaDatum["numeric_scale"];
                dataRow2["ordinal_position"] = tlisRowSchemaDatum["ordinal_position"];
                dataTable.Rows.Add(dataRow2);
            }

            tableInformationSchema._INFORMATION_SCHEMA = dataTable;
            _dicCacheSchema.Add(tcClave, tableInformationSchema);
        }

        private static DataTable preloadSchemas_ObtenerModeloDataTable(DataRow tdrMuestra)
        {
            if(preloadSchemas_TablaModelo != null)
            {
                return preloadSchemas_TablaModelo.Clone();
            }

            preloadSchemas_TablaModelo = new DataTable();
            foreach(DataColumn column2 in tdrMuestra.Table.Columns)
            {
                if(!(column2.ColumnName == "BBDD") && !(column2.ColumnName == "DATATABLE") && !(column2.ColumnName == "CLAVEDICCIONARIO"))
                {
                    DataColumn column = new DataColumn(column2.ColumnName, column2.DataType);
                    preloadSchemas_TablaModelo.Columns.Add(column);
                }
            }

            return preloadSchemas_TablaModelo.Clone();
        }

        public static bool _DeleteSchema(string tcDataBase, string tcTable)
        {
            string key = tcDataBase.Trim().ToUpper() + tcTable.Trim().ToUpper();
            if(_dicCacheSchema.ContainsKey(key))
            {
                _dicCacheSchema.Remove(key);
                return true;
            }

            return false;
        }

        public static _DBInformationSchema _DBsInformationSchema(string tcDataBase)
        {
            string key = tcDataBase.Trim().ToUpper();
            if(!_dicCacheTables.ContainsKey(key))
            {
                _DBInformationSchema value = new _DBInformationSchema(tcDataBase);
                if(!_dicCacheTables.ContainsKey(key))
                {
                    _dicCacheTables.Add(key, value);
                }
            }

            return _dicCacheTables[key];
        }

        public static void _RefreshShema(string tcDataBase)
        {
            string key = tcDataBase.Trim().ToUpper();
            _DBInformationSchema value = new _DBInformationSchema(tcDataBase);
            if(_dicCacheTables.ContainsKey(key))
            {
                _dicCacheTables.Remove(key);
            }

            _dicCacheTables.Add(key, value);
        }

        public static string _Crear_Campo_Tabla(string tcBd, string tcTabla, string tcCampo, string tcTipo = "", string tcDefault = "", string tcNulo = "NOT NULL")
        {
            string result = string.Empty;
            if(string.IsNullOrWhiteSpace(tcTipo) && string.IsNullOrWhiteSpace(tcDefault))
            {
                _Crear_Campo_Tabla_Tipo(tcCampo, ref tcTipo, ref tcDefault, ref tcNulo);
            }

            if(string.IsNullOrWhiteSpace(tcBd) || string.IsNullOrWhiteSpace(tcTabla) || string.IsNullOrWhiteSpace(tcCampo) || string.IsNullOrWhiteSpace(tcTipo))
            {
                return result;
            }

            if(!_SQLExisteCampoDB(tcBd, tcTabla, tcCampo))
            {
                result = " ALTER TABLE [" + tcBd + "].[dbo].[" + tcTabla + "]  \r\n                         ADD " + tcCampo + " " + tcTipo + " " + tcNulo + " CONSTRAINT " + _DefinicionConstraint(tcBd, tcTabla, tcCampo) + "  DEFAULT (" + tcDefault + ");" + Environment.NewLine;
            }

            return result;
        }

        private static void _Crear_Campo_Tabla_Tipo(string tcCampo, ref string tcTipo, ref string tcDefault, ref string tcNulo)
        {
            tcNulo = "NOT NULL";
            switch(tcCampo.ToLower().Trim())
            {
                case "guid_id":
                    tcTipo = "CHAR(50)";
                    tcDefault = "newid()";
                    break;
                case "created":
                case "modified":
                    tcTipo = "DATETIME";
                    tcDefault = "getdate()";
                    break;
            }
        }

        public static string _DefinicionConstraint(string tcBd, string tcTabla, string tcCampo)
        {
            return "[df__" + tcBd.ToLower() + "__" + tcTabla.ToLower() + "__" + tcCampo.ToLower() + "]";
        }

        public static bool _SQLExisteCampoDB(string tcNombreBBDD, string tcNombreTabla, string tcNombreCampo)
        {
            bool result = false;
            _ = string.Empty;
            DataTable dtTabla = new DataTable();
            if(string.IsNullOrWhiteSpace(tcNombreBBDD) || string.IsNullOrWhiteSpace(tcNombreTabla) || string.IsNullOrWhiteSpace(tcNombreCampo))
            {
                return result;
            }

            result = SQLExec("IF EXISTS (SELECT 1 FROM [master].sys.databases WHERE name = " + SQLString(tcNombreBBDD) + ")  SELECT 1 AS ExisteBBDD ELSE SELECT 0 AS ExisteBBDD ", ref dtTabla);
            if(result && dtTabla.Rows.Count > 0)
            {
                result = Convert.ToBoolean(dtTabla.Rows[0]["ExisteBBDD"]);
            }

            if(!result)
            {
                return false;
            }

            result = SQLExec("IF EXISTS (SELECT 1 FROM [" + tcNombreBBDD + "].INFORMATION_SCHEMA.COLUMNS WHERE table_name = " + SQLString(tcNombreTabla.Trim().ToLower()) + " and       column_name = " + SQLString(tcNombreCampo.Trim().ToLower()) + ") SELECT 1 AS ExisteCampo ELSE SELECT 0 AS ExisteCampo ", ref dtTabla);
            if(result && dtTabla.Rows.Count > 0)
            {
                result = Convert.ToBoolean(dtTabla.Rows[0]["ExisteCampo"]);
            }

            return result;
        }

        public static bool _CambiarLongitudCampo(string tcNombreCampo, int tnAnchoCampo, string tcRellenarCon = "", int tnDesdePosicion = -1, int tnIzquierdaDerecha = 1)
        {
            return _CambiarLongitudCampo(tcNombreCampo, tnAnchoCampo, tcRellenarCon, tnDesdePosicion, tnIzquierdaDerecha, tlPermitirAnchoSuperiorMaximo: false);
        }

        public static bool _CambiarLongitudCampo(string tcNombreCampo, int tnAnchoCampo, string tcRellenarCon = "", int tnDesdePosicion = -1, int tnIzquierdaDerecha = 1, bool tlPermitirAnchoSuperiorMaximo = false)
        {
            Error_Message = "";
            if(string.IsNullOrEmpty(tcNombreCampo.Trim()))
            {
                Error_Message = "El nombre del campo es incorrecto.";
                return false;
            }

            if(tnAnchoCampo == 0)
            {
                Error_Message = "La longitud es incorrecta.";
                return false;
            }

            _oCambioLong = new _LongitudCampo();
            _oCambioLong._Campo = tcNombreCampo.ToUpper();
            _oCambioLong._Ancho = tnAnchoCampo;
            _oCambioLong._Relleno = tcRellenarCon;
            _oCambioLong._Posicion = tnDesdePosicion;
            _oCambioLong._Lado = tnIzquierdaDerecha;
            if(true && ObtenerRegistrosConfig() && ComprobarCampoClave(tlPermitirAnchoSuperiorMaximo))
            {
                return CambiarLongitud();
            }

            return false;
        }

        private static bool ObtenerRegistrosConfig()
        {
            DataTable dataTable = null;
            foreach(KeyValuePair<string, string> item in _oAliasDB)
            {
                if((item.Key.Length >= 4 && item.Key.Substring(0, 4) == "COMU" && item.Key != "COMUNES") || !SQLExisteCampo(item.Key, "CONFIG", "CLAVE"))
                {
                    continue;
                }

                string text = " ,";
                text = text + (SQLExisteCampo(item.Key, "CONFIG", "minlong") ? "minlong" : "0 as minlong") + ",";
                text += (SQLExisteCampo(item.Key, "CONFIG", "maxlong") ? "maxlong" : "0 as maxlong");
                string text2 = " ";
                if(!string.IsNullOrEmpty(_oCambioLong._Campo.Trim()))
                {
                    text2 = " clave = '" + _oCambioLong._Campo + "' AND ";
                }

                string text3 = " ";
                if(_oCambioLong._ConfigPripal)
                {
                    text3 = " PRINCIPAL=1 AND ";
                }

                DataTable dtTabla = new DataTable();
                SQLExec("SELECT '" + item.Key + "' as BD_CONFIG, clave, principal, db, fichero, nombre " + text + " FROM " + SQLDatabase(item.Key, "CONFIG") + " where " + text2 + " " + text3 + " UPPER(db)!='VISTAS' AND (left(ltrim(rtrim(fichero)), 2)!='V_') AND not (right(ltrim(rtrim(db)), 2)='_V') Order by clave, db, fichero, nombre ", ref dtTabla);
                if(dtTabla != null && dtTabla.Rows.Count > 0)
                {
                    if(dataTable == null)
                    {
                        dataTable = dtTabla.Copy();
                    }
                    else
                    {
                        dataTable.Merge(dtTabla, preserveChanges: true);
                    }
                }
            }

            if(dataTable == null || dataTable.Rows.Count == 0)
            {
                Error_Message = "No se han podido obtener las datos de la tabla 'CONFIG'.";
                return false;
            }

            _oCambioLong._dtConfig = dataTable.Copy();
            return true;
        }

        private static bool ComprobarCampoClave(bool tlPermitirAnchoSuperiorMaximo = false)
        {
            DataRow[] array = _oCambioLong._dtConfig.Select("BD_CONFIG = 'COMUNES' and clave = '" + _oCambioLong._Campo + "' and principal");
            if(array == null || array.Count() == 0)
            {
                array = _oCambioLong._dtConfig.Select("BD_CONFIG <> 'COMUNES' and clave = '" + _oCambioLong._Campo + "' and principal");
            }

            if(array == null || array.Count() == 0)
            {
                Error_Message = "No se han encontrado registros que correspondan al campo '" + _oCambioLong._Campo + "'. No se puede continuar con el proceso.";
                return false;
            }

            string text = Convert.ToString(array[0]["db"]).Trim();
            string text2 = Convert.ToString(array[0]["fichero"]).Trim();
            string text3 = Convert.ToString(array[0]["nombre"]).Trim();
            if(string.IsNullOrEmpty(text.Trim()) || string.IsNullOrEmpty(text2.Trim()) || string.IsNullOrEmpty(text3.Trim()))
            {
                Error_Message = "No se ha podido obtener la longitud del campo '" + _oCambioLong._Campo + "'. No se puede continuar con el proceso.";
                return false;
            }

            _oCambioLong._dtClave = array.CopyToDataTable();
            if(_oCambioLong._Ancho == 0)
            {
                return true;
            }

            int num = Convert.ToInt16(array[0]["minlong"]);
            int num2 = Convert.ToInt16(array[0]["maxlong"]);
            if(_oCambioLong._Ancho < num && num > 0)
            {
                Error_Message = "La longitud mínima del campo '" + _oCambioLong._Campo + "' es de " + num + " dígitos. No se permite reducir su tamaño.";
                return false;
            }

            if(!tlPermitirAnchoSuperiorMaximo && _oCambioLong._Ancho > num2 && num2 > 0)
            {
                Error_Message = "La longitud máxima del campo '" + _oCambioLong._Campo + "' es de " + num2 + " dígitos. No se permite aumentar su tamaño.";
                return false;
            }

            return true;
        }

        private static bool CambiarLongitud()
        {
            List<string> list = new List<string>(_oAliasDBEjer.Keys);
            foreach(DataRow row in _oCambioLong._dtConfig.Rows)
            {
                string text = Convert.ToString(row["db"]).Trim();
                string text2 = Convert.ToString(row["clave"]).Trim();
                if((!string.IsNullOrEmpty(_oCambioLong._cFiltroDb) && _oCambioLong._cFiltroDb.ToUpper() != text.ToUpper()) || (!string.IsNullOrEmpty(_oCambioLong._cFiltroClave) && _oCambioLong._cFiltroClave.ToUpper() != text2.ToUpper()))
                {
                    continue;
                }

                _oCambioLong._cTabla = Convert.ToString(row["fichero"]).Trim();
                _oCambioLong._cCampo = Convert.ToString(row["nombre"]).Trim();
                int num = ((!(text == "2024XP")) ? 1 : list.Count());
                for(int i = 0; i < num; i++)
                {
                    if(text == "2024XP")
                    {
                        _oCambioLong._cDb = Convert.ToString(list[i]);
                    }
                    else
                    {
                        _oCambioLong._cDb = text;
                    }

                    _oCambioLong._cDbReal = ObtenerBdReal(_oCambioLong._cDb);
                    _oCambioLong._Ancho_Old = SQLAnchuraCampo(_oCambioLong._cDb, _oCambioLong._cTabla, _oCambioLong._cCampo);
                    if(_oCambioLong._Ancho_Old != _oCambioLong._Ancho)
                    {
                        if(string.IsNullOrEmpty(_oCambioLong._cDbReal.Trim()))
                        {
                            Error_Message = "La base de datos " + _oCambioLong._cDb + " es incorrecta.";
                            break;
                        }

                        if(!_SQLExisteTablaBBDD(_oCambioLong._cDbReal, _oCambioLong._cTabla))
                        {
                            Error_Message = "La tabla '" + _oCambioLong._cTabla + "' de la base de datos " + _oCambioLong._cDb + " no existe.";
                        }
                        else if(!SQLExisteCampo(_oCambioLong._cDb, _oCambioLong._cTabla, _oCambioLong._cCampo))
                        {
                            Error_Message = "El campo '" + _oCambioLong._cCampo + "' de la tabla '" + _oCambioLong._cTabla + "' de la base de datos " + _oCambioLong._cDb + " no existe.";
                        }
                        else
                        {
                            CambiarCampo(_oCambioLong._cDb, _oCambioLong._cDbReal, _oCambioLong._cTabla, _oCambioLong._cCampo);
                        }
                    }
                }
            }

            return true;
        }

        private static bool CambiarCampo(string tcDb, string tcDBReal, string tcTabla, string tcCampo)
        {
            string text = "";
            string text2 = "";
            CambiarCampoAnterior();
            BorrarIndicesCampo();
            text2 = " NOT NULL ";
            if(SQLCampoPermiteNulos(_oCambioLong._cDb, _oCambioLong._cTabla, _oCambioLong._cCampo))
            {
                text2 = " ";
            }

            text = " ALTER TABLE [" + _oCambioLong._cDbReal + "].dbo." + _oCambioLong._cTabla + " ALTER COLUMN [" + _oCambioLong._cCampo + "] CHAR(" + _oCambioLong._Ancho + ") COLLATE Modern_Spanish_CS_AI " + text2;
            string lcMsg = "Error modificando longitud del campo " + _oCambioLong._cCampo + " de la tabla " + _oCambioLong._cTabla + " de la base de datos " + _oCambioLong._cDb + ".";
            EjecutarConsultaCambio(text, lcMsg);
            CrearIndicesCampo();
            CambiarCampoRelleno();
            CambiarCampoPosterior();
            return true;
        }

        private static bool CambiarCampoAnterior()
        {
            string text = "";
            bool result = true;
            if(_oCambioLong._Ancho_Old > _oCambioLong._Ancho)
            {
                string campo = _oCambioLong._Campo;
                if(!(campo == "CUENTAS"))
                {
                    if(campo == "OBRA")
                    {
                        text = " UPDATE [" + _oCambioLong._cDbReal + "].dbo." + _oCambioLong._cTabla + " SET " + _oCambioLong._cCampo + "= RIGHT(" + _oCambioLong._cCampo + ", " + _oCambioLong._Ancho + ") WHERE " + SQLAlltrim(_oCambioLong._cCampo) + " !=''  ";
                    }
                }
                else
                {
                    _ = _oCambioLong._Ancho;
                    text = " UPDATE [" + _oCambioLong._cDbReal + "].dbo." + _oCambioLong._cTabla + " SET " + _oCambioLong._cCampo + "= LEFT(" + _oCambioLong._cCampo + ", 4) + RIGHT(" + SQLAlltrim(_oCambioLong._cCampo) + ", " + (_oCambioLong._Ancho - 4) + ")  WHERE " + SQLAlltrim(_oCambioLong._cCampo) + " !=''  AND LEN(" + SQLAlltrim(_oCambioLong._cCampo) + ") > 4";
                }
            }

            if(!string.IsNullOrEmpty(text.Trim()))
            {
                string lcMsg = "Error modificando longitud del campo " + _oCambioLong._cCampo + " de la tabla " + _oCambioLong._cTabla + " de la base de datos " + _oCambioLong._cDb + ".";
                result = EjecutarConsultaCambio(text, lcMsg);
            }

            return result;
        }

        private static bool CambiarCampoPosterior()
        {
            string text = "";
            string text2 = "";
            string cCampo = _oCambioLong._cCampo;
            bool result = true;
            if(!string.IsNullOrEmpty(_oCambioLong._Relleno))
            {
                return true;
            }

            if(_oCambioLong._Ancho <= _oCambioLong._Ancho_Old)
            {
                return true;
            }

            switch(_oCambioLong._Campo)
            {
                case "CUENTAS":
                    text2 = string.Concat(Enumerable.Repeat("0", _oCambioLong._Ancho - _oCambioLong._Ancho_Old));
                    text = " UPDATE [" + _oCambioLong._cDbReal + "].dbo." + _oCambioLong._cTabla + " SET " + cCampo + " = LEFT(" + cCampo + ", 4) + '" + text2 + "' + RIGHT(" + cCampo + ", " + (_oCambioLong._Ancho_Old - 4) + ") WHERE " + SQLAlltrim(_oCambioLong._cCampo) + " !='' AND LEN(" + SQLAlltrim(cCampo) + ") > 4";
                    break;
                case "FACTURA_COMPRA":
                    text2 = string.Concat(Enumerable.Repeat(" ", _oCambioLong._Ancho - _oCambioLong._Ancho_Old));
                    text = " UPDATE [" + _oCambioLong._cDbReal + "].dbo." + _oCambioLong._cTabla + "  SET " + cCampo + " = '" + text2 + "' + " + cCampo + " WHERE " + SQLAlltrim(_oCambioLong._cCampo) + " !=''";
                    switch(_oCambioLong._cTabla)
                    {
                        case "FICHEROS":
                        case "ASI_ULT":
                            text += " AND (LEFT(PROVEEDOR, 2) = '40' OR LEFT(PROVEEDOR, 2) = '41')";
                            break;
                        case "IVA_EXEN":
                        case "MODREGIS":
                        case "RETREPER":
                            text += " AND (LEFT(CUENTA, 2) = '40' OR LEFT(CUENTA, 2) = '41')";
                            break;
                        case "PREVIOBS":
                            text += " AND (LEFT(PROVECLIEN, 2) = '40' OR LEFT(PROVECLIEN, 2) = '41')";
                            break;
                    }

                    break;
                case "OBRA":
                    text2 = string.Concat(Enumerable.Repeat("0", _oCambioLong._Ancho - _oCambioLong._Ancho_Old));
                    text = " UPDATE [" + _oCambioLong._cDbReal + "].dbo." + _oCambioLong._cTabla + "  SET " + cCampo + " = '" + text2 + "' + " + cCampo + " WHERE " + SQLAlltrim(_oCambioLong._cCampo) + " !=''";
                    break;
            }

            if(!string.IsNullOrEmpty(text.Trim()))
            {
                string lcMsg = "Error modificando longitud del campo " + _oCambioLong._cCampo + " de la tabla " + _oCambioLong._cTabla + " de la base de datos " + _oCambioLong._cDb + ".";
                result = EjecutarConsultaCambio(text, lcMsg);
            }

            return result;
        }

        private static bool CambiarCampoRelleno()
        {
            string text = "";
            string cCampo = _oCambioLong._cCampo;
            string text2 = Convert.ToString(_oCambioLong._Posicion).Trim();
            if(_oCambioLong._Campo.Trim().ToUpper() != "FACTURA-COMPRA")
            {
                if(string.IsNullOrEmpty(_oCambioLong._Relleno))
                {
                    return true;
                }
            }
            else
            {
                _oCambioLong._Relleno = " ";
            }

            if(_oCambioLong._Ancho - _oCambioLong._Ancho_Old <= 0)
            {
                return true;
            }

            string text3 = _oCambioLong._Relleno;
            if(text3.Length == 1)
            {
                text3 = string.Concat(Enumerable.Repeat(text3, _oCambioLong._Ancho - _oCambioLong._Ancho_Old));
            }

            text = ((_oCambioLong._Lado != 1) ? (" UPDATE [" + _oCambioLong._cDbReal + "].dbo." + _oCambioLong._cTabla + " SET " + cCampo + "= RTRIM(LTRIM(LEFT(" + SQLAlltrim(cCampo) + ", LEN(" + cCampo + ") - " + text2 + "))) + RTRIM(LTRIM('" + text3 + "')) + RIGHT(" + SQLAlltrim(cCampo) + "," + text2 + ") WHERE " + SQLAlltrim(_oCambioLong._cCampo) + " <>'' AND (LEN(" + cCampo + ")-" + text2 + " >= 0)") : ((!(_oCambioLong._Campo.Trim().ToUpper() == "FACTURA-COMPRA")) ? (" UPDATE [" + _oCambioLong._cDbReal + "].dbo." + _oCambioLong._cTabla + " SET " + cCampo + "= RTRIM(LTRIM(LEFT(" + cCampo + ", " + text2 + ")))  + RTRIM(LTRIM('" + text3 + "')) + RIGHT(" + SQLAlltrim(cCampo) + ", LEN(" + cCampo + ")-" + text2 + ") WHERE " + SQLAlltrim(_oCambioLong._cCampo) + " <>'' AND (LEN(" + cCampo + ")-" + text2 + " >= 0)") : string.Concat(" UPDATE [", _oCambioLong._cDbReal, "].dbo.", _oCambioLong._cTabla, " SET ", cCampo, "= '", string.Concat(Enumerable.Repeat(" ", _oCambioLong._Ancho - _oCambioLong._Ancho_Old)), "'+", _oCambioLong._cCampo)));
            if(_oCambioLong._Campo == "CUENTAS")
            {
                text = text + " AND LEN(" + SQLAlltrim(cCampo) + ") > 4";
            }

            string lcMsg = "Error modificando valor del campo " + cCampo + " de la tabla " + _oCambioLong._cTabla + " de la base de datos " + _oCambioLong._cDb + ".";
            EjecutarConsultaCambio(text, lcMsg);
            return true;
        }

        private static bool ObtenerIndicesTabla(string tcDBReal, ref DataTable tdtIndices)
        {
            if(!_oCambioLong._dicIndices.ContainsKey(tcDBReal))
            {
                _DbIndicesCampos(tcDBReal, ref tdtIndices);
                _oCambioLong._dicIndices.Add(tcDBReal, tdtIndices);
            }
            else
            {
                _oCambioLong._dicIndices.TryGetValue(tcDBReal, out tdtIndices);
            }

            if(tdtIndices == null || tdtIndices.Rows.Count == 0)
            {
                return false;
            }

            return true;
        }

        private static bool BorrarIndicesCampo()
        {
            DataTable tdtIndices = new DataTable();
            if(!ObtenerIndicesTabla(_oCambioLong._cDbReal, ref tdtIndices))
            {
                return true;
            }

            DataRow[] array = tdtIndices.Select("tabla = '" + _oCambioLong._cTabla + "' and columna = '" + _oCambioLong._cCampo + "'");
            if(array == null || array.Count() == 0)
            {
                return true;
            }

            string text = "";
            DataRow[] array2 = array;
            foreach(DataRow obj in array2)
            {
                bool flag = Convert.ToBoolean(obj["indice_primario"]);
                string text2 = Convert.ToString(obj["indice"]).Trim();
                text = ((!flag) ? (text + "DROP INDEX dbo." + _oCambioLong._cTabla + "." + text2 + ";") : (text + "ALTER TABLE [" + _oCambioLong._cDbReal + "].dbo." + _oCambioLong._cTabla + " DROP CONSTRAINT " + text2 + " ;"));
            }

            if(!string.IsNullOrEmpty(text.Trim()))
            {
                text = "use [" + _oCambioLong._cDbReal + "]; " + text;
                string lcMsg = "Error eliminando los índices de la tabla " + _oCambioLong._cTabla + " de la base de datos " + _oCambioLong._cDb + ".";
                EjecutarConsultaCambio(text, lcMsg);
            }

            return true;
        }

        private static bool CrearIndicesCampo()
        {
            bool flag = false;
            string text = "";
            string text2 = "";
            string text3 = "";
            DataTable tdtIndices = new DataTable();
            if(!ObtenerIndicesTabla(_oCambioLong._cDbReal, ref tdtIndices))
            {
                return true;
            }

            DataRow[] array = tdtIndices.Select("tabla='" + _oCambioLong._cTabla + "' and columna = '" + _oCambioLong._cCampo + "'");
            if(array.GetLength(0) == 0)
            {
                return true;
            }

            DataRow[] array2 = array;
            for(int i = 0; i < array2.Length; i++)
            {
                int num = Convert.ToInt16(array2[i]["indice_id"]);
                DataRow[] array3 = tdtIndices.Select("tabla='" + _oCambioLong._cTabla + "' and indice_id = " + num, "indice_orden asc");
                int j = 0;
                while(j < array3.GetLength(0))
                {
                    text2 = Convert.ToString(array3[j]["indice"]).Trim();
                    flag = Convert.ToBoolean(array3[j]["indice_primario"]);
                    text3 = "";
                    for(; j < array3.GetLength(0) && Convert.ToString(array3[j]["indice"]).Trim() == text2; j++)
                    {
                        text3 = text3 + " [" + Convert.ToString(array3[j]["columna"]).Trim() + "] ASC, ";
                    }

                    if(!string.IsNullOrWhiteSpace(text3))
                    {
                        text3 = text3.Trim();
                        text3 = text3.Substring(0, text3.Length - 1);
                    }

                    text = ((!flag) ? (text + "CREATE NONCLUSTERED INDEX [" + text2 + "] ON [" + _oCambioLong._cDbReal + "].[dbo].[" + _oCambioLong._cTabla + "] (" + text3 + ") WITH (STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [eurowini]; ") : (text + " ALTER TABLE [" + _oCambioLong._cDbReal + "].[dbo].[" + _oCambioLong._cTabla + "] ADD CONSTRAINT [" + text2 + "]  PRIMARY KEY CLUSTERED ( " + text3 + ")  WITH ( STATISTICS_NORECOMPUTE = OFF,  IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  ON [eurowind]; "));
                }
            }

            if(!string.IsNullOrEmpty(text.Trim()))
            {
                string lcMsg = "Error creando los índices de la tabla " + _oCambioLong._cTabla + " de la base de datos " + _oCambioLong._cDb + ".";
                EjecutarConsultaCambio(text, lcMsg);
            }

            return true;
        }

        private static bool CampoPermiteNulos(string tcDb, string tcDBReal, string tcTabla, string tcCampo)
        {
            DataTable tdtTablasCampos = new DataTable();
            if(!_oCambioLong._dicTablasCampos.ContainsKey(tcDBReal))
            {
                _DbTablasCampos(tcDBReal, ref tdtTablasCampos);
                _oCambioLong._dicTablasCampos.Add(tcDBReal, tdtTablasCampos);
            }
            else
            {
                _oCambioLong._dicTablasCampos.TryGetValue(tcDBReal, out tdtTablasCampos);
            }

            if(tdtTablasCampos == null || tdtTablasCampos.Rows.Count == 0)
            {
                return false;
            }

            return tdtTablasCampos.Select("tabla='" + tcTabla + "' and campo = '" + tcCampo + "' and permite_nulo").Length != 0;
        }

        private static bool EjecutarConsultaCambio(string lcSql, string lcMsg)
        {
            if(!string.IsNullOrWhiteSpace(lcSql.Trim()) && !SQLExec(lcSql))
            {
                Error_Message = lcMsg;
                if(_oCambioLong._log)
                {
                    Escribir_En_Log_Error(Modo_Registro.Registro_Error, null, lcMsg);
                }

                return false;
            }

            return true;
        }

        private static string ObtenerBdReal(string tcBd)
        {
            string value = "";
            _oAliasDB.TryGetValue(tcBd, out value);
            if(string.IsNullOrEmpty(value))
            {
                _oAliasDB.TryGetValue(tcBd.Trim().ToUpper(), out value);
            }

            if(string.IsNullOrEmpty(value))
            {
                return "";
            }

            int num = value.LastIndexOf("[");
            int num2 = value.LastIndexOf("]");
            if(num >= 0 && num2 > 0 && num2 > num)
            {
                value = value.Substring(num + 1, num2 - num - 1);
            }

            return value;
        }

        public static object _GetVariable(string nombrevar)
        {
            return _GetVariable(nombrevar, null);
        }

        public static object _GetVariable(string nombrevar, object toValorPorDefecto = null)
        {
            object value;
            if(string.IsNullOrWhiteSpace(nombrevar))
            {
                value = toValorPorDefecto;
            }
            else if(!_VarGlob.TryGetValue(nombrevar.ToLower(), out value))
            {
                if(toValorPorDefecto != null)
                {
                    return toValorPorDefecto;
                }

                value = null;
                switch(nombrevar.ToLower().Trim().Substring(0, 3))
                {
                    case "wl_":
                        value = false;
                        break;
                    case "wn_":
                        value = 0;
                        break;
                    case "wc_":
                        value = "";
                        break;
                }
            }

            return value;
        }

        public static bool _SetVariable(string tcNombreVariable, object toValor)
        {
            if(string.IsNullOrWhiteSpace(tcNombreVariable))
            {
                return false;
            }

            string key = tcNombreVariable.Trim().ToLower();
            if(!_VarGlob.ContainsKey(key))
            {
                return false;
            }

            _VarGlob[key] = toValor;
            return true;
        }

        public static bool _ObtenerCamposAmpliados(ref Dictionary<string, int> _dicCamposAmplia)
        {
            _oCambioLong = new _LongitudCampo();
            _oCambioLong._Campo = "";
            _oCambioLong._ConfigPripal = true;
            if(!ObtenerRegistrosConfig())
            {
                return false;
            }

            ComprobarCamposAmpliados(ref _dicCamposAmplia);
            return true;
        }

        private static void ComprobarCamposAmpliados(ref Dictionary<string, int> _dicCamposAmplia)
        {
            List<string> list = new List<string>(_oAliasDBEjer.Keys);
            foreach(DataRow row in _oCambioLong._dtConfig.Rows)
            {
                string text = Convert.ToString(row["clave"]).Trim();
                string text2 = Convert.ToString(row["db"]).Trim();
                string tcTabla = Convert.ToString(row["fichero"]).Trim();
                string tcCampo = Convert.ToString(row["nombre"]).Trim();
                int num = Convert.ToInt16(row["minlong"]);
                if(text.ToUpper().Trim() == "FACTURA-COMPRA")
                {
                    num = 20;
                }

                if(num != 0 && !_dicCamposAmplia.ContainsKey(text))
                {
                    string tcDatabaseLogica = ((!(text2 == "2024XP")) ? text2 : Convert.ToString(list[0]));
                    if(SQLAnchuraCampo(tcDatabaseLogica, tcTabla, tcCampo) != num)
                    {
                        _dicCamposAmplia.Add(text, num);
                    }
                }
            }
        }

        public static bool CompararBds(string tcBdOrigen, string tcBdDestino)
        {
            oCompareEst = new CompareBD();
            oCompareEst.bdorigen = tcBdOrigen;
            oCompareEst.bddestino = tcBdDestino;
            oCompareEst.bdlogicorigen = AñadirDicAlias(tcBdOrigen);
            oCompareEst.bdlogicdestino = AñadirDicAlias(tcBdDestino);
            EstructuraLongitudCampos(tcBdOrigen);
            EstructuraGuardarLog("Comparación estructuras, origen: " + tcBdOrigen + " , destino: " + tcBdDestino);
            _DBInformationSchema dBInformationSchema = new _DBInformationSchema(tcBdOrigen);
            _DBInformationSchema dBInformationSchema2 = new _DBInformationSchema(tcBdDestino);
            foreach(DataRow row in dBInformationSchema._INFORMATION_SCHEMA_TABLES.Rows)
            {
                oCompareEst.tabla = Convert.ToString(row["table_name"]);
                if(dBInformationSchema2._INFORMATION_SCHEMA_TABLES.Select("table_name = '" + oCompareEst.tabla + "'").Length == 0)
                {
                    EstructuraGuardarLog("Crear tabla: " + oCompareEst.tabla);
                    if(!CrearNuevaTabla())
                    {
                        EstructuraGuardarLog("Creando tabla " + oCompareEst.MsgError, tlError: true);
                    }
                }
                else
                {
                    EstructuraGuardarLog("Comparar tabla: " + oCompareEst.tabla);
                    if(!CompararTablas())
                    {
                        EstructuraGuardarLog("Comparando tabla " + oCompareEst.MsgError, tlError: true);
                    }
                }
            }

            if(oCompareEst.error)
            {
                MessageBox.Show("Se han producido errores durante el proceso de comparar las bases de datos." + Environment.NewLine + "Revisar el fichero log para más detalles " + Environment.NewLine + oCompareEst.ficheroLog);
            }

            return !oCompareEst.error;
        }

        private static bool CompararTablas()
        {
            string DefCampos = "";
            string DefConstraints = "";
            string DeleteConstraints = "";
            string tcDefConstraints = "";
            string tcIndices = "";
            string text = oCompareEst.tabla.ToLower();
            DataTable iNFORMATION_SCHEMA = _TablesInformationSchema(oCompareEst.bdlogicorigen, text)._INFORMATION_SCHEMA;
            DataTable iNFORMATION_SCHEMA2 = _TablesInformationSchema(oCompareEst.bdlogicdestino, text)._INFORMATION_SCHEMA;
            string tcBd = oCompareEst.bdorigen.ToLower();
            string text2 = oCompareEst.bddestino.ToLower();
            foreach(DataRow row in iNFORMATION_SCHEMA.Rows)
            {
                Campo campo = DatosCampo(row);
                campo.tabla = text;
                campo.basedatos = text2;
                CompararCampos(campo, iNFORMATION_SCHEMA2, ref DefCampos, ref DefConstraints, ref DeleteConstraints);
            }

            if(string.IsNullOrEmpty(DefCampos))
            {
                return true;
            }

            BorrarIndicesTabla(text2, text, ref tcIndices);
            CrearIndicesTabla(tcBd, text2, text, ref tcDefConstraints);
            bool num = SQLExec(string.Concat(string.Concat(string.Concat(string.Concat(string.Concat("USE [" + text2 + "]" + Environment.NewLine, tcIndices), DeleteConstraints), DefCampos), tcDefConstraints), DefConstraints));
            if(!num)
            {
                oCompareEst.MsgError = "Error comparar tabla " + text + Environment.NewLine + Error_Message;
            }

            return num;
        }

        private static bool CompararCampos(Campo toCampo, DataTable dtCampos, ref string DefCampos, ref string DefConstraints, ref string DeleteConstraints)
        {
            string DefCampo = "";
            string DefConstraint = "";
            string DeleteConstraint = "";
            string messageError = "";
            int num = 0;
            DataRow[] array = dtCampos.Select("column_name = '" + toCampo.nombre + "'");
            if(array.Count() == 0)
            {
                num = 1;
            }
            else
            {
                Campo campo = DatosCampo(array[0]);
                if(toCampo.tipo != campo.tipo || toCampo.nulo != campo.nulo || toCampo.longitud != campo.longitud || toCampo.defecto != campo.defecto || toCampo.precision != campo.precision || toCampo.decimales != campo.decimales)
                {
                    num = 2;
                }
            }

            object obj;
            switch(num)
            {
                case 0:
                    return true;
                default:
                    obj = "Modificar campo ";
                    break;
                case 1:
                    obj = "Crear campo ";
                    break;
            }

            EstructuraGuardarLog((string)obj + toCampo.nombre);
            if(!ObtenerDefinicionCampo(num, toCampo, ref DefCampo, ref DefConstraint, ref DeleteConstraint, out messageError))
            {
                EstructuraGuardarLog("Definición campo " + toCampo.nombre + ", " + messageError, tlError: true);
            }

            if(!string.IsNullOrEmpty(DefConstraint))
            {
                DefConstraints = DefConstraints + DefConstraint + ";" + Environment.NewLine;
            }

            if(!string.IsNullOrEmpty(DeleteConstraint))
            {
                DeleteConstraints = DeleteConstraints + DeleteConstraint + ";" + Environment.NewLine;
            }

            if(!string.IsNullOrEmpty(DefCampo))
            {
                DefCampos = DefCampos + DefCampo + ";" + Environment.NewLine;
            }

            return true;
        }

        private static bool CrearNuevaTabla()
        {
            string text = "";
            string text2 = "";
            string text3 = "";
            string text4 = "";
            string tcDefConstraints = "";
            string DefCampo = "";
            string DefConstraint = "";
            string DeleteConstraint = "";
            string messageError = "";
            DataTable iNFORMATION_SCHEMA = _TablesInformationSchema(oCompareEst.bdlogicorigen, oCompareEst.tabla)._INFORMATION_SCHEMA;
            string tcBd = oCompareEst.bdorigen.ToLower();
            string text5 = oCompareEst.bddestino.ToLower();
            string text6 = oCompareEst.tabla.ToLower();
            if(string.IsNullOrWhiteSpace(text6))
            {
                oCompareEst.MsgError = "Debe especificar el nombre de la nueva tabla.";
                return false;
            }

            if(_SQLExisteTablaBBDD(text5, text6))
            {
                oCompareEst.MsgError = "Ya existe una tabla con el mismo nombre.";
                return false;
            }

            if(iNFORMATION_SCHEMA.Rows.Count == 0)
            {
                oCompareEst.MsgError = "Falta añadir las definiciones de los campos.";
                return false;
            }

            foreach(DataRow row in iNFORMATION_SCHEMA.Rows)
            {
                Campo campo = DatosCampo(row);
                campo.tabla = text6;
                campo.basedatos = text5;
                if(ObtenerDefinicionCampo(0, campo, ref DefCampo, ref DefConstraint, ref DeleteConstraint, out messageError))
                {
                    text += ((!string.IsNullOrEmpty(text)) ? (", " + Environment.NewLine) : "");
                    text += DefCampo;
                    if(!string.IsNullOrEmpty(DefConstraint))
                    {
                        text2 = text2 + DefConstraint + "; " + Environment.NewLine;
                    }

                    if(!string.IsNullOrEmpty(DeleteConstraint))
                    {
                        text3 = text3 + DeleteConstraint + "; " + Environment.NewLine;
                    }
                }
                else
                {
                    EstructuraGuardarLog("Definición campo " + campo.nombre + ", " + messageError, tlError: true);
                }
            }

            CrearIndicesTabla(tcBd, text5, text6, ref tcDefConstraints);
            bool num = SQLExec(string.Concat(string.Concat(string.Concat(string.Concat(string.Concat(string.Concat(string.Concat(string.Concat(string.Concat(string.Concat(string.Concat("USE [" + text5 + "]" + Environment.NewLine, "SET ANSI_NULLS OFF", Environment.NewLine), "SET QUOTED_IDENTIFIER ON", Environment.NewLine), "SET ANSI_PADDING ON", Environment.NewLine), "SET ANSI_NULLS OFF", Environment.NewLine), "CREATE TABLE [dbo].[", text6, "]("), text), Environment.NewLine), ") ON [eurowind]", text4, Environment.NewLine), "SET ANSI_PADDING ON", Environment.NewLine), text2), tcDefConstraints));
            if(!num)
            {
                oCompareEst.MsgError = Error_Message;
            }

            return num;
        }

        private static bool ObtenerDefinicionCampo(int tnAccion, Campo toCampo, ref string DefCampo, ref string DefConstraint, ref string DeleteConstraint, out string messageError)
        {
            string text = "";
            string text2 = "";
            bool result = true;
            messageError = "";
            DefCampo = "[" + toCampo.nombre + "] ";
            if(string.IsNullOrWhiteSpace(toCampo.nombre))
            {
                messageError = "Falta indicar el nombre del campo.";
                return false;
            }

            switch(toCampo.tipobase)
            {
                case "caracter":
                    if(toCampo.longitud == 0)
                    {
                        messageError = messageError + "Falta indicar la longitud del campo " + toCampo.nombre + "." + Environment.NewLine;
                        result = false;
                    }

                    DefCampo += "[char]";
                    DefCampo = DefCampo + "(" + toCampo.longitud + ") COLLATE Modern_Spanish_CS_AI ";
                    text = (string.IsNullOrEmpty(toCampo.defecto) ? "('')" : toCampo.defecto);
                    break;
                case "logico":
                    DefCampo += "[bit] ";
                    toCampo.defecto = (string.IsNullOrEmpty(toCampo.defecto) ? "(0)" : toCampo.defecto);
                    text = (toCampo.defecto.Contains("(") ? toCampo.defecto : ("(" + toCampo.defecto + ")"));
                    break;
                case "memo":
                    DefCampo += "[text] COLLATE Modern_Spanish_CS_AI ";
                    text = (string.IsNullOrEmpty(toCampo.defecto) ? "('')" : toCampo.defecto);
                    break;
                case "entero":
                    DefCampo += "[int] ";
                    toCampo.defecto = (string.IsNullOrEmpty(toCampo.defecto) ? "(0)" : toCampo.defecto);
                    text = (toCampo.defecto.Contains("(") ? toCampo.defecto : ("(" + toCampo.defecto + ")"));
                    break;
                case "fecha":
                    DefCampo += "[datetime] ";
                    text = (string.IsNullOrEmpty(toCampo.defecto) ? "(getdate())" : toCampo.defecto);
                    break;
                case "numerico":
                    if(toCampo.precision == 0)
                    {
                        messageError = messageError + "Falta indicar la longitud del campo " + toCampo.nombre + "." + Environment.NewLine;
                        result = false;
                    }

                    if(toCampo.decimales > toCampo.precision)
                    {
                        messageError = messageError + "Los decimales del campo " + toCampo.nombre + " no pueden ser mayores que su longitud." + Environment.NewLine;
                        result = false;
                    }

                    DefCampo += "[numeric]";
                    DefCampo = DefCampo + "(" + toCampo.precision + ", " + toCampo.decimales + ") ";
                    toCampo.defecto = (string.IsNullOrEmpty(toCampo.defecto) ? "(0)" : toCampo.defecto);
                    text = (toCampo.defecto.Contains("(") ? toCampo.defecto : ("(" + toCampo.defecto + ")"));
                    break;
            }

            text2 = ((!toCampo.nulo) ? (text2 + "NOT NULL") : (text2 + "NULL"));
            string text3 = DefinicioConstraint(toCampo.basedatos, toCampo.tabla, toCampo.nombre);
            DefConstraint = " ALTER TABLE [dbo].[" + toCampo.tabla + "] ADD  CONSTRAINT " + text3 + "  DEFAULT (" + text + ") FOR [" + toCampo.nombre + "]";
            DeleteConstraint = " ALTER TABLE [dbo].[" + toCampo.tabla + "] DROP CONSTRAINT " + text3;
            switch(tnAccion)
            {
                case 1:
                    DefCampo = " ALTER TABLE [dbo].[" + toCampo.tabla + "] ADD " + DefCampo + " CONSTRAINT " + text3 + " DEFAULT " + text + " " + text2;
                    DefConstraint = "";
                    DeleteConstraint = "";
                    break;
                case 2:
                    DefCampo = " ALTER TABLE [dbo].[" + toCampo.tabla + "] ALTER COLUMN " + DefCampo + " " + text2;
                    break;
                case 3:
                    DefCampo = " " + DefCampo + " " + text2;
                    break;
            }

            return result;
        }

        private static string DefinicioConstraint(string tcBd, string tcTabla, string tcCampo)
        {
            return "[df__" + tcBd + "__" + tcTabla + "__" + tcCampo.ToLower() + "]";
        }

        private static bool CrearIndicesTabla(string tcBd, string tcNewBd, string tcTabla, ref string tcDefConstraints)
        {
            bool flag = false;
            string text = "";
            string text2 = "";
            string text3 = "";
            DataTable tdtIndices = new DataTable();
            _DbIndicesCampos(tcBd, ref tdtIndices);
            tdtIndices.DefaultView.RowFilter = "tabla='" + tcTabla + "'";
            DataTable dataTable = tdtIndices.DefaultView.ToTable(true, "indice_id");
            if(dataTable.Rows.Count == 0)
            {
                return true;
            }

            foreach(DataRow row in dataTable.Rows)
            {
                int num = Convert.ToInt16(row["indice_id"]);
                DataRow[] array = tdtIndices.Select("tabla='" + tcTabla + "' and indice_id = " + num, "indice_orden asc");
                int i = 0;
                while(i < array.GetLength(0))
                {
                    text2 = Convert.ToString(array[i]["indice"]).Trim();
                    flag = Convert.ToBoolean(array[i]["indice_primario"]);
                    text3 = "";
                    for(; i < array.GetLength(0) && Convert.ToString(array[i]["indice"]).Trim() == text2; i++)
                    {
                        text3 = text3 + " [" + Convert.ToString(array[i]["columna"]).Trim() + "] ASC, ";
                    }

                    if(!string.IsNullOrWhiteSpace(text3))
                    {
                        text3 = text3.Trim();
                        text3 = text3.Substring(0, text3.Length - 1);
                    }

                    text2 = text2.Replace(tcBd.ToLower(), tcNewBd.ToLower());
                    text2 = text2.Replace(tcBd.ToUpper(), tcNewBd.ToLower());
                    text = ((!flag) ? (text + "CREATE NONCLUSTERED INDEX [" + text2 + "] ON [" + tcNewBd + "].[dbo].[" + tcTabla + "] (" + text3 + ") WITH (STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [eurowini]; ") : (text + " ALTER TABLE [" + tcNewBd + "].[dbo].[" + tcTabla + "] ADD CONSTRAINT [" + text2 + "]  PRIMARY KEY CLUSTERED ( " + text3 + ")  WITH ( STATISTICS_NORECOMPUTE = OFF,  IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  ON [eurowind]; "));
                    text += Environment.NewLine;
                }
            }

            tcDefConstraints = ((!string.IsNullOrEmpty(text.Trim())) ? text : "");
            return true;
        }

        private static bool BorrarIndicesTabla(string tcBd, string tcTabla, ref string tcIndices)
        {
            DataTable tdtIndices = new DataTable();
            _DbIndicesCampos(tcBd, ref tdtIndices);
            tdtIndices.DefaultView.RowFilter = "tabla='" + tcTabla + "'";
            DataTable dataTable = tdtIndices.DefaultView.ToTable(true, "indice", "indice_primario");
            if(dataTable.Rows.Count == 0)
            {
                return true;
            }

            tcIndices = "";
            foreach(DataRow row in dataTable.Rows)
            {
                bool flag = Convert.ToBoolean(row["indice_primario"]);
                string text = Convert.ToString(row["indice"]).Trim();
                if(flag)
                {
                    tcIndices = tcIndices + "ALTER TABLE [" + tcBd + "].dbo." + tcTabla + " DROP CONSTRAINT " + text + " ;";
                }
                else
                {
                    tcIndices = tcIndices + "DROP INDEX dbo." + tcTabla + "." + text + ";";
                }

                tcIndices += Environment.NewLine;
            }

            return true;
        }

        private static Campo DatosCampo(DataRow dtRow, string tcBd = "", string tcTabla = "")
        {
            Campo obj = new Campo
        {
                nombre = Convert.ToString(dtRow["column_name"]),
                tipo = Convert.ToString(dtRow["data_type"])
            };
            obj.tipobase = ObtenerTipoCampoEstructura(obj.tipo);
            obj.nulo = Convert.ToString(dtRow["is_nullable"]) == "YES";
            obj.longitud = ((!DBNull.Value.Equals(dtRow["character_maximum_length"])) ? Convert.ToInt32(dtRow["character_maximum_length"]) : 0);
            obj.defecto = Convert.ToString(dtRow["column_default"]);
            obj.precision = ((!DBNull.Value.Equals(dtRow["numeric_precision"])) ? Convert.ToInt16(dtRow["numeric_precision"]) : 0);
            obj.decimales = ((!DBNull.Value.Equals(dtRow["numeric_scale"])) ? Convert.ToInt16(dtRow["numeric_scale"]) : 0);
            obj.posicion = Convert.ToInt16(dtRow["ordinal_position"]);
            obj.basedatos = tcBd;
            obj.tabla = tcTabla;
            return obj;
        }

        private static string ObtenerTipoCampoEstructura(string tcTipoCampoSqlServer)
        {
            string result = "";
            switch(tcTipoCampoSqlServer)
            {
                case "bit":
                    result = "logico";
                    break;
                case "text":
                case "ntext":
                    result = "memo";
                    break;
                case "char":
                case "varchar":
                case "nchar":
                case "nvarchar":
                    result = "caracter";
                    break;
                case "date":
                case "datetimeoffset":
                case "datetime2":
                case "smalldatetime":
                case "datetime":
                case "time":
                    result = "fecha";
                    break;
                case "bigint":
                case "numeric":
                case "smallint":
                case "decimal":
                case "smallmoney":
                case "tinyint":
                case "float":
                case "real":
                    result = "numerico";
                    break;
                case "int":
                    result = "entero";
                    break;
            }

            return result;
        }

        private static string AñadirDicAlias(string tcBd, string tcKey = "")
        {
            string text = ExisteDicAlias(tcBd);
            if(string.IsNullOrEmpty(text))
            {
                text = (string.IsNullOrEmpty(tcKey) ? tcBd : tcKey).ToUpper();
                _oAliasDB.Add(text, "[" + tcBd + "].dbo.");
            }

            return text;
        }

        private static string ExisteDicAlias(string tcBd)
        {
            string text = "[" + tcBd + "].dbo.";
            string result = "";
            foreach(KeyValuePair<string, string> item in _oAliasDB)
            {
                if(item.Value.ToLower().Trim() == text.ToLower().Trim())
                {
                    result = item.Key;
                    break;
                }
            }

            return result;
        }

        private static void AñadirDicAliasKey(string tcKey, string tcBd = "")
        {
            tcBd = (string.IsNullOrEmpty(tcBd) ? tcKey : tcBd);
            string value = "[" + tcBd + "].dbo.";
            if(!_oAliasDB.ContainsKey(tcKey.ToUpper()))
            {
                _oAliasDB.Add(tcKey.ToUpper(), value);
            }
        }

        public static bool EstructuraLongitudCampos(string tcBd)
        {
            bool flag = false;
            string text = "";
            string text2 = "";
            string text3 = "";
            AñadirDicAliasKey(tcBd);
            text3 = tcBd.ToUpper();
            _oCambioLong = new _LongitudCampo();
            _oCambioLong._cFiltroDb = text3;
            flag = ObtenerRegistrosConfig();
            if(!flag)
            {
                return false;
            }

            DataRow[] array = _oCambioLong._dtConfig.Select("BD_CONFIG = '" + text3 + "'", "clave");
            array.ToList().ForEach(delegate (DataRow r) {
                r.SetField("Db", tcBd);
            });
            DataRow[] array2 = array;
            foreach(DataRow obj in array2)
            {
                text = Convert.ToString(obj["clave"]).Trim();
                Convert.ToString(obj["fichero"]).Trim();
                Convert.ToString(obj["nombre"]).Trim();
                _oCambioLong._Ancho = 0;
                if(string.IsNullOrEmpty(text2) || !(text == text2))
                {
                    _oCambioLong._Ancho = ObtenerLongitudCampo(_oCambioLong._dtConfig, text);
                    if(_oCambioLong._Ancho > 0)
                    {
                        _oCambioLong._cFiltroClave = text;
                        CambiarLongitud();
                    }

                    text2 = text;
                }
            }

            array2 = array;
            foreach(DataRow dataRow in array2)
            {
                _DeleteSchema(tcBd, Convert.ToString(dataRow["fichero"]).Trim());
            }

            return flag;
        }

        private static int ObtenerLongitudCampo(DataTable tdConfig, string tcClave)
        {
            _oCambioLong._Campo = tcClave;
            if(!ComprobarCampoClave())
            {
                return 0;
            }

            if(_oCambioLong._dtClave == null || _oCambioLong._dtClave.Rows.Count == 0)
            {
                return 0;
            }

            string tcDatabaseLogica = Convert.ToString(_oCambioLong._dtClave.Rows[0]["db"]).Trim();
            string tcTabla = Convert.ToString(_oCambioLong._dtClave.Rows[0]["fichero"]).Trim();
            string tcCampo = Convert.ToString(_oCambioLong._dtClave.Rows[0]["nombre"]).Trim();
            return SQLAnchuraCampo(tcDatabaseLogica, tcTabla, tcCampo);
        }

        private static void EstructuraGuardarLog(string tcMsg, bool tlError = false)
        {
            if(string.IsNullOrEmpty(oCompareEst.ficheroLog))
            {
                oCompareEst.ficheroLog = EstructuraFicheroLog();
            }

            tcMsg = (tlError ? "(ERROR)" : "") + tcMsg;
            StreamWriter streamWriter = null;
            using(streamWriter = new StreamWriter(File.Open(oCompareEst.ficheroLog, FileMode.Append), Encoding.GetEncoding("iso-8859-1")))
            {
                streamWriter.WriteLine("{0} : {1}", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), tcMsg);
            }

            oCompareEst.error = oCompareEst.error || tlError;
        }

        private static string EstructuraFicheroLog()
        {
            string text = _GetVariable("wc_iniservidor").ToString();
            if(string.IsNullOrEmpty(text) || !Directory.Exists(text))
            {
                text = System.IO.Path.GetTempPath();
            }

            if(!text.EndsWith("\\"))
            {
                text += "\\";
            }

            text += "\\LOGS";
            if(!Directory.Exists(text))
            {
                Directory.CreateDirectory(text);
            }

            string text2 = DateTime.Now.Date.ToString("yyyyMMdd");
            string text3 = DateTime.Now.ToLongTimeString().Replace(":", "");
            return text + "\\log_comparar_" + text2 + "_" + text3 + ".txt";
        }

        public static bool Restore_Indices_ValoresDefecto(string tcDbNombreViejo, string tcDbNombreNuevo)
        {
            DataTable tdtIndices = new DataTable();
            if(_DbIndicesCampos(tcDbNombreNuevo, ref tdtIndices) && !Restore_Indices(tcDbNombreViejo, tcDbNombreNuevo, "", tdtIndices))
            {
                return false;
            }

            DataTable tdtValDefecto = new DataTable();
            if(_DbValoresDefectoCampos(tcDbNombreNuevo, ref tdtValDefecto) && !Restore_ValoresDefecto(tcDbNombreViejo, tcDbNombreNuevo, "", tdtValDefecto))
            {
                return false;
            }

            return true;
        }

        private static bool Restore_Indices(string tcBdOrigen, string tcBdNueva, string tcTabla, DataTable tdtIndices)
        {
            string text = "";
            string text2 = "";
            string text3 = "";
            string text4 = "";
            tcTabla = tcTabla.ToLower().Trim();
            tcBdNueva = tcBdNueva.ToLower().Trim();
            tcBdOrigen = tcBdOrigen.ToLower().Trim();
            text3 = "indice_primario " + ((!string.IsNullOrEmpty(tcTabla.Trim())) ? (" and tabla='" + tcTabla + "'") : "");
            DataRow[] array = new DataView(tdtIndices).ToTable(true, "tabla", "indice", "indice_primario").Select(text3, "tabla, indice asc");
            for(int i = 0; i < array.Length; i++)
            {
                text2 = Convert.ToString(array[i]["indice"]).Trim().ToLower();
                text4 = text2.Replace(tcBdOrigen, tcBdNueva);
                text = text + " exec sp_rename '" + text2 + "', '" + text4 + "', 'object'";
            }

            if(!string.IsNullOrWhiteSpace(text))
            {
                text = "use [" + tcBdNueva + "]; " + text;
                if(!SQLExec(text))
                {
                    if(string.IsNullOrEmpty(tcTabla))
                    {
                        Error_Message = "Error al renombrar los índices de la base de datos " + tcBdNueva;
                    }
                    else
                    {
                        Error_Message = "Error al renombrar los índices para la tabla " + tcTabla + " de la base de datos " + tcBdNueva;
                    }

                    return false;
                }
            }

            return true;
        }

        private static bool Restore_ValoresDefecto(string tcBdOrigen, string tcBdNueva, string tcTabla, DataTable tdtValsDefecto)
        {
            string text = "";
            string text2 = "";
            string text3 = "";
            string text4 = "";
            tcTabla = tcTabla.ToLower().Trim();
            tcBdNueva = tcBdNueva.ToLower().Trim();
            tcBdOrigen = tcBdOrigen.ToLower().Trim();
            text3 = ((!string.IsNullOrEmpty(tcTabla.Trim())) ? ("tabla='" + tcTabla + "'") : "");
            DataRow[] array = tdtValsDefecto.Select(text3);
            for(int i = 0; i < array.Length; i++)
            {
                text2 = Convert.ToString(array[i]["constr_nombre"]).Trim().ToLower();
                text4 = text2.Replace(tcBdOrigen, tcBdNueva);
                text = text + " exec sp_rename '" + text2 + "', '" + text4 + "', 'object'";
            }

            if(!string.IsNullOrWhiteSpace(text))
            {
                text = "use [" + tcBdNueva + "]; " + text;
                if(!SQLExec(text))
                {
                    if(string.IsNullOrEmpty(tcTabla))
                    {
                        Error_Message = "Error al renombrar los valores por defecto de la base de datos " + tcBdNueva;
                    }
                    else
                    {
                        Error_Message = "Error al renombrar los valores por defecto para la tabla " + tcTabla + " de la base de datos " + tcBdNueva;
                    }

                    return false;
                }
            }

            return true;
        }

        public static bool CreateScriptBd(string tcBd, List<string> tablasExc = null, bool tlExportarDatos = true, string tcFile = "")
        {
            string text = "#DATABASENAME#";
            string text2 = "";
            string tcDefCampos = "";
            string tcDefConstraints = "";
            string text3 = "";
            string text4 = "";
            string text5 = "";
            string text6 = "";
            if(!_SQLExisteBBDD(tcBd))
            {
                Error_Message = "La base de datos " + tcBd + " no existe.";
                return false;
            }

            text6 = AñadirDicAlias(tcBd);
            foreach(DataRow row in new _DBInformationSchema(tcBd)._INFORMATION_SCHEMA_TABLES.Rows)
            {
                text2 = Convert.ToString(row["table_name"]);
                DataTable iNFORMATION_SCHEMA = _TablesInformationSchema(text6, text2)._INFORMATION_SCHEMA;
                CreateScriptCampos(tcBd, iNFORMATION_SCHEMA, text2, ref tcDefCampos, ref tcDefConstraints);
                text4 = "";
                if(tlExportarDatos && (tablasExc == null || !tablasExc.Contains(text2)))
                {
                    CreateScriptValues(tcBd, iNFORMATION_SCHEMA, text6, text2, ref text4);
                }

                text3 = "";
                CrearIndicesTabla(tcBd, text, text2, ref text3);
                text5 = text5 + "/****** Crear Tabla " + text2 + " ******/" + Environment.NewLine + Environment.NewLine;
                text5 = text5 + "USE [" + text + "]" + Environment.NewLine;
                text5 = text5 + "SET ANSI_NULLS OFF" + Environment.NewLine;
                text5 = text5 + "SET QUOTED_IDENTIFIER ON" + Environment.NewLine;
                text5 = text5 + "SET ANSI_PADDING ON" + Environment.NewLine + Environment.NewLine;
                text5 = text5 + " CREATE TABLE [dbo].[" + text2 + "] " + Environment.NewLine;
                text5 = text5 + "(" + tcDefCampos + ")" + Environment.NewLine;
                text5 = text5 + " ON [eurowind]; " + Environment.NewLine + Environment.NewLine;
                text5 = text5 + "SET ANSI_PADDING ON" + Environment.NewLine + Environment.NewLine;
                text5 = text5 + tcDefConstraints + Environment.NewLine + Environment.NewLine;
                text5 = text5 + text3 + ((!string.IsNullOrEmpty(text3)) ? (Environment.NewLine + Environment.NewLine) : "");
                if(!string.IsNullOrEmpty(text4))
                {
                    text5 = text5 + "SET ANSI_PADDING ON" + Environment.NewLine + Environment.NewLine;
                    text5 = text5 + text4 + Environment.NewLine + Environment.NewLine;
                }
            }

            text5 = Regex.Replace(text5, tcBd, text, RegexOptions.IgnoreCase);
            tcFile = (string.IsNullOrEmpty(tcFile) ? (System.IO.Path.GetTempPath().TrimEnd() + "CreateBD.sql") : tcFile);
            File.WriteAllText(tcFile, text5);
            return true;
        }

        private static bool CreateScriptCampos(string tcBd, DataTable dtCampos, string lcTabla, ref string tcDefCampos, ref string tcDefConstraints)
        {
            string DefCampo = "";
            string DefConstraint = "";
            string DeleteConstraint = "";
            string messageError = "";
            tcDefCampos = "";
            tcDefConstraints = "";
            foreach(DataRow row in dtCampos.Rows)
            {
                Campo campo = DatosCampo(row);
                campo.tabla = lcTabla;
                campo.basedatos = tcBd;
                if(ObtenerDefinicionCampo(3, campo, ref DefCampo, ref DefConstraint, ref DeleteConstraint, out messageError))
                {
                    tcDefCampos += ((!string.IsNullOrEmpty(tcDefCampos)) ? (", " + Environment.NewLine) : "");
                    tcDefCampos += DefCampo;
                    if(!string.IsNullOrEmpty(DefConstraint))
                    {
                        tcDefConstraints = tcDefConstraints + DefConstraint + "; " + Environment.NewLine;
                    }
                }
            }

            return true;
        }

        private static bool CreateScriptValues(string tcBd, DataTable dtCampos, string tcBdLogic, string lcTabla, ref string tcInsert)
        {
            string text = "";
            string tcValues = "";
            tcInsert = "";
            string[] array = new string[dtCampos.Rows.Count];
            for(int i = 0; i < dtCampos.Rows.Count; i++)
            {
                Campo campo = DatosCampo(dtCampos.Rows[i]);
                text = text + "[" + campo.nombre + "], ";
                array[i] = campo.nombre;
            }

            text = text.Substring(0, text.Length - 2);
            DataTable dtTabla = new DataTable();
            SQLExec(" SELECT " + text + " FROM " + SQLDatabase(tcBdLogic, lcTabla), ref dtTabla);
            if(dtTabla == null || dtTabla.Rows.Count == 0)
            {
                return false;
            }

            foreach(DataRow row in dtTabla.Rows)
            {
                CreateScriptGetValue(row, array, ref tcValues);
                tcInsert = tcInsert + "INSERT [dbo].[" + lcTabla + "] (" + text + ") VALUES (" + tcValues + ") " + Environment.NewLine;
            }

            return true;
        }

        private static bool CreateScriptGetValue(DataRow drReg, string[] taCampos, ref string tcValues)
        {
            tcValues = "";
            for(int i = 0; i < taCampos.Count(); i++)
            {
                string text = SQLString(drReg[taCampos[i]]);
                tcValues = tcValues + text + ", ";
            }

            tcValues = tcValues.Substring(0, tcValues.Length - 2);
            return true;
        }

        public static bool CreateBdDesdeScript(string tcBd, string tcScript)
        {
            if(string.IsNullOrEmpty(tcBd))
            {
                Error_Message = "El nombre de la base de datos no es válido.";
                return false;
            }

            if(string.IsNullOrEmpty(tcScript))
            {
                Error_Message = "La definición de la base de datos no es válida.";
                return false;
            }

            tcBd = tcBd.Trim();
            if(_SQLExisteBBDD(tcBd))
            {
                Error_Message = "La base de datos " + tcBd + " ya existe.";
                return false;
            }

            if(!_DBCreate(tcBd))
            {
                Error_Message = "No se ha podido crear la base de datos " + tcBd + ".";
                return false;
            }

            tcScript = Regex.Replace(tcScript, "#DATABASENAME#", tcBd.ToLower(), RegexOptions.IgnoreCase);
            if(SQLExec(tcScript))
            {
                return true;
            }

            Error_Message = "No se ha podido crear la estructura de la base de datos " + tcBd + "." + ((!string.IsNullOrWhiteSpace(Error_Message)) ? (Environment.NewLine + Environment.NewLine + "Error: " + Environment.NewLine + Error_Message) : "");
            _DBRemove(new SqlConnection(Conexion), tcBd.Trim());
            return false;
        }
    }
}
#if false // Decompilation log
'83' items in cache
------------------
Resolve: 'mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Found single assembly: 'mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Load from: 'C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.8\mscorlib.dll'
------------------
Resolve: 'System.Data, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Found single assembly: 'System.Data, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Load from: 'C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.8\System.Data.dll'
------------------
Resolve: 'System.Runtime.Caching, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a'
Could not find by name: 'System.Runtime.Caching, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a'
------------------
Resolve: 'System, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Found single assembly: 'System, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Load from: 'C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.8\System.dll'
------------------
Resolve: 'System.Windows.Forms, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Found single assembly: 'System.Windows.Forms, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Load from: 'C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.8\System.Windows.Forms.dll'
------------------
Resolve: 'System.Data.DataSetExtensions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Found single assembly: 'System.Data.DataSetExtensions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Load from: 'C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.8\System.Data.DataSetExtensions.dll'
------------------
Resolve: 'System.Core, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Found single assembly: 'System.Core, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Load from: 'C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.8\System.Core.dll'
------------------
Resolve: 'System.Xml, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Found single assembly: 'System.Xml, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089'
Load from: 'C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.8\System.Xml.dll'
------------------
Resolve: 'System.Drawing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a'
Found single assembly: 'System.Drawing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a'
Load from: 'C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.8\System.Drawing.dll'
#endif
