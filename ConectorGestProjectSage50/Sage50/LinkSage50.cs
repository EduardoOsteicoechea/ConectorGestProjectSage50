using System;
using System.IO;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Globalization;
using System.Threading;
using System.Runtime.InteropServices;

// Clases de Sage50
//using sage._50;
using sage.ew.db;
using sage.ew.functions;
using sage.ew.global;
//using sage.ew.global.Clases;
using sage.ew.usuario;

using sage.ew.functions.Clases;

using ExampleSatelite.Sage50.Datos;

/// 22/09/2022
using sage.ew.objetos;
using sage.ew.interficies;
using sage.ew.empresa;
//using Sage.ES.S50.Addons;
using sage.ew.ewbase;
using sage._50;
using Sage.ES.S50.Addons;
using System.Windows.Forms;

namespace ExampleSatelite.Sage50.Negocio
{
    public class LinkSage50
    {
        #region propiedades
        public clsDatos _oDatos = new clsDatos();

        public bool _Loaded_Ok = false;
        public bool _connected = false;
        public bool _EsMultiEmpresa = false;

        public string _Empresa = "";
        public string _Ejercicio = "";
        public string _Comunes = "";
        public string _ComunesPrincipal = "";

        public string _Error_Message = "";

        public string _Terminal = "";
        private string _DllTerminal = "";

        #endregion propiedades

        #region constructor
        public LinkSage50()
        {
        }

        public LinkSage50(String tsTerminal)
        {
            if(!string.IsNullOrEmpty(tsTerminal))
            {
                this._Terminal = tsTerminal;
                this._DllTerminal = Path.Combine(this._Terminal, @"Librerias\");

                this._AssemblyResolveBegin();

                this._Loaded_Ok = true;
            }
        }
        #endregion constructor

        /// <summary>
        /// Conectar a Sage50, está conexión usa una licencia de uso.
        /// </summary>
        /// <param name="tsUser"></param>
        /// <param name="tsPass"></param>
        /// <param name="tsEmpresa"></param>
        /// <returns></returns>
        public Boolean _Connect(string tsUser, string tsPass, string tsEmpresa = "")
        {
            //_connected = main_s50.Connect(@"C:\Sage50c\sage50cterm\", "SUPERVISOR", "1");
            _connected = main_s50.Connect(this._Terminal, tsUser, tsPass, tsEmpresa);

            if(_connected == true)
            {
                _EsMultiEmpresa = this._HaveMultiCompany();
            }

            return _connected;
        }


        /// <summary>
        /// Desconectar de Sage50
        /// </summary>
        public void _Disconnect()
        {
            if(_connected == true)
            {
                // Para que Sage50 interprete que está desconectado.
                ReplicatedDB.Conexion = "";
                _connected = false;
            }
            this._AssemblyResolveEnd();
        }

        /// <summary>
        /// Cargar las variables globales que usa sage50
        /// </summary>
        /// <param name="tcEmpresa"></param>
        /// <returns></returns>
        public Boolean _LoadGlobalVariables(string tsEmpresa = "")
        {
            Boolean llOk = false;
            string lsEmpresa = this._Empresa;

            if(!string.IsNullOrEmpty(tsEmpresa.Trim()))
                lsEmpresa = tsEmpresa;

            llOk = main_s50._Cargar_Globales(lsEmpresa);

            if(llOk)
            {
                // Obtenemos el valor de las variables globales cargadas en la empresa
                _Empresa = EW_GLOBAL._GetVariable("wc_empresa").ToString();
                _Ejercicio = EW_GLOBAL._GetVariable("wc_any").ToString();
                _Comunes = ReplicatedDB.DbComunes.ToString();
                //

                _LoadEnvironmentCompany();
            }

            return llOk;
        }

        /// <summary>
        /// Cargar el entorno trabajo de la empresa
        /// </summary>
        /// <param name="tsEmpresa"></param>
        public void _LoadEnvironmentCompany(string tsEmpresa = "")
        {
            //EW_GLOBAL._Empresa = new sage.ew.empresa.Empresa(Convert.ToString(EW_GLOBAL._GetVariable("wc_empresa")));
            string lsEmpresa = this._Empresa;

            if(!string.IsNullOrEmpty(tsEmpresa.Trim()))
                lsEmpresa = tsEmpresa;

            Usuario._This._Cambiar_Empresa(lsEmpresa);


            // Obtenemos el valor de las variables globales cargadas en la empresa
            _Empresa = EW_GLOBAL._GetVariable("wc_empresa").ToString();
            _Ejercicio = EW_GLOBAL._GetVariable("wc_any").ToString();
            _Comunes = ReplicatedDB.DbComunes.ToString();
            //
        }

        /// <summary>
        /// Cambio de ejercicio de la empresa actual
        /// </summary>
        /// <param name="tscEjercicio"></param>
        public void _ExerciseChange(string tscEjercicio)
        {
            // sage.ew.functions.Clases.CambioEjercicio ol = new sage.ew.functions.Clases.CambioEjercicio();

            sage.ew.functions.Clases.CambioEjercicio loEjer = new sage.ew.functions.Clases.CambioEjercicio();
            loEjer._Cambiar(tscEjercicio);

            _LoadEnvironmentCompany();
        }


        /// 22/09/2022
        /// <summary>
        /// Cambio de grupo de empresa.
        /// Datos necesarios del Grupo origen -> Codigo Empresa Actual, Ejercicio Actual de la sesión.
        /// Se verifica que el usuario actual, tenga acceso al grupo destino
        /// Se verifica que la empresa del grupo origen, exista en el grupo destino.. si no es así, se utilizará la primera empresa del grupo destino
        /// Se verifica que el ejercicio actual del grupo origen, este creado en el grupo destino, si no es así, utilizará el ultimo ejercicio disponible en el grupo destino
        /// </summary>
        public Boolean _GroupCompanyChange(string tscGrupoEmpresa)
        {
            bool llOk = false;

            #region solo para la parte Visual (addons)
            //// Si tenemos el diseñador de Sage 50 en modo de edición no podemos cambiar de usaurio
            //ControladorDesktop loControlador = (ControladorDesktop)EW_GLOBAL._GetVariable("wo_ControladorDesktop");

            //if (loControlador != null && loControlador._StateEditionDesktop())
            //{
            //    this._Error_Message = "Para cambiar de grupo de empresa es necesario guardar primero el diseño de escritoio actual.";

            //    return llOk;
            //}

            //// Mirar que no existen formularios abiertos
            //if (!FUNCTIONS._ComprobarFormulariosAbiertos())
            //{
            //    this._Error_Message = "Se ha detectado que tiene formularios abiertos." + System.Environment.NewLine +
            //                          "Para cambiar de grupo de empresa no puede haber ningún formulario abierto.";

            //    return llOk;
            //}
            #endregion solo para la parte Visual (addons)

            string lcActGrupoEmp = string.Empty, lcNewGrupoEmp = string.Empty;
            string lcActEmpresa = string.Empty, lcNewEmpresa = string.Empty;
            string lcActEjercicio = string.Empty, lcNewEjercicio = string.Empty;
            bool tlRefrescar = false; // true;   // Si se ha de refrescar el escritorio.

            // Grupo Actual
            lcActGrupoEmp = ReplicatedDB.DbComunes.Trim().Substring(4, 4);
            lcActEmpresa = Convert.ToString(EW_GLOBAL._GetVariable("wc_empresa"));
            lcActEjercicio = Convert.ToString(EW_GLOBAL._GetVariable("wc_any"));

            // Grupo Destino
            lcNewGrupoEmp = tscGrupoEmpresa;
            lcNewEmpresa = lcActEmpresa;
            lcNewEjercicio = lcActEjercicio;

            GrupoEmpresaSel _oGruposEmp = new GrupoEmpresaSel();

            if(_oGruposEmp.ExisteUsuario(lcNewGrupoEmp))
            {
                // Comprobar si tenemos empresa y en caso contrario seleccionar una empresa que tenga acceso
                if(!string.IsNullOrWhiteSpace(lcNewEmpresa) && _oGruposEmp.VerificarEmpresaAcceso(lcNewGrupoEmp, lcNewEmpresa, ref lcNewEmpresa))
                {
                    // Comprobar si existe el usuario en el nuevo grupo
                    if(_oGruposEmp.AccesoUsuarioEmpresa(lcNewGrupoEmp, lcNewEmpresa))
                    {
                        // Variables para controlar el cambio de grupo desde dentro de la propia pantalla frmLogin que se va a mostrar caso
                        // de que el usuario tenga password diferente en el grupo origen y el grupo destino.
                        //
                        Usuario._This._GrupoCambiadoEnFrmLogin = "";  // Se le da valor solo cuando al cambiar de grupo desde dentro de frmLogin con el boton de seleccion de grupos (browser de grupos).


                        // Si el password en el grupo al que vamos no coincide con el password del usuario en el grupo en el que estamos,
                        // o,
                        // si la empresa del grupo al que vamos es una empresa oculta (con password),
                        // pedimos identificación al usuario.
                        //
                        if(!_oGruposEmp._ComprobarPassword(lcNewGrupoEmp) || _oGruposEmp.EmpresaOcultaEnGrupo(lcNewGrupoEmp, lcNewEmpresa))
                        {
                            llOk = _oGruposEmp._CambiarGrupo(lcNewGrupoEmp, lcNewEmpresa, false);

                            #region solo para la parte Visual (addons)
                            if(!Usuario._This._ShowLoginCambioGrupoEmpresas(ref lcNewEmpresa, ref tlRefrescar))
                            {
                                _oGruposEmp._CambiarGrupo(lcActGrupoEmp, lcActEmpresa, false);
                                llOk = false;
                                //return null;
                            }
                            else
                            { llOk = true; }
                            #endregion solo para la parte Visual (addons)
                        }

                        #region solo para la parte Visual (addons)
                        // Si se cambió de grupo desde dentro del frmLogin me aseguro de ir hacia él, en el cambio de grupo que hago
                        // a continuación en la siguiente linea.
                        //
                        if(!string.IsNullOrWhiteSpace(Usuario._This._GrupoCambiadoEnFrmLogin))
                            lcNewGrupoEmp = Usuario._This._GrupoCambiadoEnFrmLogin;
                        #endregion solo para la parte Visual (addons)

                        AddonsController.Instance.Methods._CambioEmpresa(TipoExecute.Before, lcActEmpresa, lcNewEmpresa);

                        llOk = _oGruposEmp._CambiarGrupo(lcNewGrupoEmp, lcNewEmpresa, tlRefrescar);

                        if(llOk)
                        {
                            AddonsController.Instance.Methods._CambioEmpresa(TipoExecute.After, lcActEmpresa, lcNewEmpresa);

                            if(EW_GLOBAL._Empresa == null)
                                EW_GLOBAL._Empresa = new Empresa(Convert.ToString(EW_GLOBAL._GetVariable("wc_empresa")));

                            return llOk; //null
                        }
                        else
                            this._Error_Message = "Se ha producido un error intentando cambiar al grupo de empresa " + lcNewGrupoEmp + ".";
                    }
                    else
                        this._Error_Message = "El usuario actual no tiene acceso a la empresa del grupo seleccionado, seleccione otra empresa que tenga acceso.";
                }
                else
                    this._Error_Message = "El usuario actual no tiene acceso a ninguna de las empresas del grupo seleccionado, no se puede cambiar al grupo de empresa seleccionado.";
            }
            else
            {
                this._Error_Message = "El usuario actual no existe en el grupo seleccionado, no se puede cambiar al grupo de empresa seleccionado.";
            }

            return llOk;
        }

        /// <summary>
        /// Cambiar de grupo de empresas
        /// </summary>
        public Boolean CambiarGrupoEmpresa(string tscGrupoEmpresa)
        {
            bool llOk = false;
            string lcEmpresa = "";
            string lcGrupoActual = GrupoEmpresa._CodigoGrupoActual();

            //Segun si tenemos la variable _cGrupoEmpresa o no cargamos el grupo actual o el que tenemos en la variable (si viene por tarea programada la funcion _Init llenará esta variable)
            string lcGrupoEjer;
            if(!string.IsNullOrWhiteSpace(tscGrupoEmpresa))
                lcGrupoEjer = tscGrupoEmpresa;
            else
                lcGrupoEjer = lcGrupoActual;

            // Si estamos en el mismo grupo salimos
            if(lcGrupoActual == lcGrupoEjer)
                return llOk;

            GrupoEmpresa _oGrupoEmpresa = new GrupoEmpresa(lcGrupoEjer);

            if(_oGrupoEmpresa._dtEmpresasGrupo != null && _oGrupoEmpresa._dtEmpresasGrupo.Rows.Count > 0)
                lcEmpresa = Convert.ToString(_oGrupoEmpresa._dtEmpresasGrupo.Rows[0]["codigo"]);

            GrupoEmpresaSel loGrupo = new GrupoEmpresaSel();
            llOk = loGrupo._CambiarGrupo(lcGrupoEjer, lcEmpresa);

            return llOk;
        }

        /// <summary>
        /// Comprobamos si Sage50 trabaja con multi-empresa
        /// </summary>
        /// <returns></returns>
        public Boolean _HaveMultiCompany()
        {
            Boolean llOk = false;
            DataTable ldtGrupos = new DataTable();
            string lcComunesPripal = "";
            string lcCodPrin = "";

            // Obtener el comunes del grupo principal correspondiente al grupo en que estoy.
            lcCodPrin = GrupoempTools._Obtener_CodGrupoPripal(ReplicatedDB.DbComunes.Trim().Substring(4, 4));
            if(!String.IsNullOrEmpty(lcCodPrin.Trim()))
            {
                lcComunesPripal = "COMU" + lcCodPrin;
                llOk = true;
            }
            else
            {
                lcComunesPripal = ReplicatedDB.DbComunes.Trim();
                llOk = false;
            }


            if(llOk == true)
            {
                llOk = _oDatos.Grupos_Multiempresa(lcCodPrin, ref ldtGrupos);
                ldtGrupos.Dispose();
            }

            //_Comunes = lcComunesPripal;
            _ComunesPrincipal = lcComunesPripal;

            return llOk;
        }


        #region Tratamiento de DLL dentro de otra carpeta

        public void _AssemblyResolveBegin()
        {
            if(!string.IsNullOrEmpty(_DllTerminal))
            {
                // Creamos el Evento para cargar la dll desde una carpeta diferente
                AppDomain currentDomain = AppDomain.CurrentDomain;
                currentDomain.AssemblyResolve += new ResolveEventHandler(this.__CurrentDomain_AssemblyResolve);
            }
        }

        public void _AssemblyResolveEnd()
        {
            AppDomain currentDomain = AppDomain.CurrentDomain;
            currentDomain.AssemblyResolve -= new ResolveEventHandler(this.__CurrentDomain_AssemblyResolve);
        }

        /// <summary>
        /// Sobrecarga del evento AssemblyResolve para poder realizar la carga de nuestras dlls desde otra carpeta
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        private Assembly __CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {
            // Nos saltamos las llamadas a recursos de dlls, ya que ya los tenemos incluidos en memoria nuestras dlls o exe.
            //string lcFile = args.Name.Replace(".dll", "");
            //lcFile = lcFile.Replace(".exe", "");
            string lcFile = args.Name.Replace(".exe", "");


            string lcDllName = args.Name.Contains(',') ? args.Name.Substring(0, args.Name.IndexOf(',')) : lcFile;

            lcDllName = lcDllName.Replace(".", "_");
            if(lcDllName.EndsWith("_resources"))
            {
                return default(Assembly);
            }

            // Comprobamos que la librería no esté ya cargada
            Assembly loAssembly = this.__GetAssemblyByName(args.Name);
            if(loAssembly != null)
            {
                return loAssembly;
            }

            // En caso de no tener cargada la librería que se quiere utilizar, le indicamos la ruta de donde obtenerla 
            try
            {
                return this.__LoadAssemblyFromPath(args.Name, this._DllTerminal);
            }
            catch(Exception)
            {
                return default(Assembly);
            }
        }

        /// <summary>
        /// Comprobar si una librería ya está cargada y devolverla 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private Assembly __GetAssemblyByName(string name)
        {
            return AppDomain.CurrentDomain.GetAssemblies().
                   SingleOrDefault(assembly => assembly.GetName().Name == name);
        }

        /// <summary>
        /// Cargar librerías de la ruta especificada
        /// </summary>
        /// <param name="assemblyName"></param>
        /// <param name="directoryPath"></param>
        /// <returns></returns>
        private Assembly __LoadAssemblyFromPath(string assemblyName, string directoryPath)
        {
            string lsExtFile = "";

            foreach(string file in Directory.GetFiles(directoryPath))
            {
                lsExtFile = Path.GetExtension(file).ToLower();

                // Solo exe y dll
                if(lsExtFile == ".dll" || lsExtFile == ".exe")
                {
                    Assembly assembly;
                    if(this.__TryLoadAssemblyFromFile(file, assemblyName, out assembly))
                    {
                        return assembly;
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Cargar librería desde la ruta especificada
        /// </summary>
        /// <param name="file"></param>
        /// <param name="assemblyName"></param>
        /// <param name="assembly"></param>
        /// <returns></returns>
        private bool __TryLoadAssemblyFromFile(string file, string assemblyName, out Assembly assembly)
        {
            try
            {
                // Comparamos solo la parte del nombre del ensamblado en Sage50, ya que en ejecución
                // existe alguno que no lo tiene bien aplicado.

                assemblyName += ",";
                string[] lcArgs0 = assemblyName.Split(',');

                // Convert the filename into an absolute file name for use with LoadFile.
                file = new FileInfo(file).FullName;

                string assemblyFile = AssemblyName.GetAssemblyName(file).FullName.ToString();
                assemblyFile += ",";
                string[] lcArgs1 = assemblyFile.Split(',');

                //if (AssemblyName.GetAssemblyName(file).FullName == assemblyName)
                if(lcArgs0[0] == lcArgs1[0])
                {
                    assembly = Assembly.LoadFile(file);
                    return true;
                }
            }
            catch
            {
            }

            assembly = null;
            return false;
        }

        #endregion Tratamiento de DLL dentro de otra carpeta

    }

    public class LinkFuncSage50
    {
        #region propiedades
        public clsDatos _oDatos = new clsDatos();

        public string _Error_Message = "";

        #endregion propiedades

        #region constructor
        public LinkFuncSage50()
        {
        }
        #endregion constructor

        // _paisempresa, Obtenemos el pais de la empresa
        public string _CountryCompany()
        {
            string lcPais;

            lcPais = ReplicatedDB.SQLValor("CODIGOS", "EMPRESA", EW_GLOBAL._GetVariable("wc_empresa").ToString(), "PAIS").ToString();
            lcPais = (String.IsNullOrWhiteSpace(lcPais) ? "034" : lcPais);

            return lcPais;
        }

        // _comprobarpais, Comprobamos el pais de la empresa
        public string _VerificateCountry(string pcPais)
        {
            string lcPais = "";

            if(String.IsNullOrWhiteSpace(pcPais))
                lcPais = this._CountryCompany();
            else
            {
                MessageBox.Show("BEFORE: \n\n lcPais = ReplicatedDB.SQLValor(\"PAISES\", \"CODIGO\", pcPais, \"CODIGO\", \"COMUNES\").ToString();");
                //lcPais = ReplicatedDB.SQLValor("PAISES", "CODIGO", pcPais, "CODIGO", "COMUNES").ToString();
                lcPais = ReplicatedDB.SQLValor("paises", "CODIGO", pcPais, "CODIGO", "COMU0001").ToString();
            }
            return lcPais.Trim();
        }

        // _comprobarcodpos, Comprovamos el código postal
        public string _VerificatePostalCode(string pcCodpos)
        {
            string lcCodpos = "";

            if(!String.IsNullOrWhiteSpace(pcCodpos))
                lcCodpos = ReplicatedDB.SQLValor("CODPOS", "CODIGO", pcCodpos, "CODIGO").ToString();

            return lcCodpos.Trim();
        }

        // _comprobartipo_iva, comprobamos el tipo de Iva
        public string _VerificateTaxType(string pcTipo_iva)
        {
            string lcTipo_iva = "";

            if(!String.IsNullOrWhiteSpace(pcTipo_iva))
                lcTipo_iva = ReplicatedDB.SQLValor("TIPO_IVA", "CODIGO", pcTipo_iva, "CODIGO").ToString();

            return lcTipo_iva.Trim();
        }

        // _comprobartipo_ret, comprobamos el tipo de retención
        public string _VerificateRetentionType(string pcTipo_ret)
        {
            string lcTipo_ret = "";

            if(!String.IsNullOrWhiteSpace(pcTipo_ret))
                lcTipo_ret = ReplicatedDB.SQLValor("TIPO_RET", "CODIGO", pcTipo_ret, "CODIGO").ToString();

            return lcTipo_ret.Trim();
        }

        // _comprobarformapago, comprobamos la forma de pago
        public string _VerificatePaymentMethod(string pcFormapago)
        {
            string lcFormapago = "";

            if(!String.IsNullOrWhiteSpace(pcFormapago))
                lcFormapago = ReplicatedDB.SQLValor("FPAG", "CODIGO", pcFormapago, "CODIGO").ToString();

            return lcFormapago.Trim();
        }

        // _comprobarnifcliente, comprobamos los NIF de cliente
        // regresa un diccionario con los diferentes registros que tengan el mismo NIF
        public Dictionary<string, object> _VerificateNIFCustomer(string pcNif)
        {
            Dictionary<string, object> loResultado = new Dictionary<string, object>();

            if(!String.IsNullOrWhiteSpace(pcNif))
                loResultado = ReplicatedDB.SQLREGValor("CLIENTES", "cif", pcNif);

            return loResultado;
        }

        // _comprobarnifproveedor, comprobamos los NIF de cliente
        // regresa un diccionario con los diferentes registros que tengan el mismo NIF
        public Dictionary<string, object> _VerificateNIFProvider(string pcNif)
        {
            Dictionary<string, object> loResultado = new Dictionary<string, object>();

            if(!String.IsNullOrWhiteSpace(pcNif))
                loResultado = ReplicatedDB.SQLREGValor("PROVEED", "cif", pcNif);

            return loResultado;
        }

    }

}
