using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

// Sage50
using sage.ew.global;
using sage.ew.docsven;
using sage.ew.docscompra;
using sage.ew.cliente;
using sage.ew.articulo;
using sage.ew.lote;
using sage.ew.lote.Clases; 

// propio
using ExampleSatelite.Sage50.Datos;

namespace ExampleSatelite.Sage50.Negocio
{
    public class BaseDocument
    {
        
        // Empresa actual
        private string _PrivateEmpresa = EW_GLOBAL._GetVariable("wc_empresa").ToString();
        public string _Empresa { get { return _PrivateEmpresa; } }

        // Ejercicio actual
        private string _PrivateEjercicio = EW_GLOBAL._GetVariable("wc_Any").ToString();
        public string _Ejercicio { get { return _PrivateEjercicio; } }

        // Código de cliente varios
        private string _PrivateCliVarios = EW_GLOBAL._GetVariable("wc_clivarios").ToString();
        public string _CliVarios { get { return _PrivateCliVarios; } }

        // Código de almacén predeterminado
        private string _PrivateAlmacen = EW_GLOBAL._GetVariable("wc_almacen").ToString();
        public string _Almacen { get { return _PrivateAlmacen; } }

        // Código de Moneda predeterminado
        private string _PrivateMoneda = EW_GLOBAL._GetVariable("wc_moneda").ToString();
        public string _Moneda { get { return _PrivateMoneda; } }


        // Cfg.Empresa. Flag -> no dejar vender en caso de no tener stock
        private Boolean _PrivateFlagStock = Convert.ToBoolean(EW_GLOBAL._GetVariable("wl_stock"));
        public Boolean _FlagStock { get { return _PrivateFlagStock; } }



        public clsDatos loDatos = new clsDatos();
        public string _Error_Message = "";

        public string Error()
        {
            return this._Error_Message.Trim();
        }

        /// <summary>
        /// Llama al _PrecioVenta de esta forma aquesta se puede sobreescribir en el documento.
        /// </summary>
        /// <param name="toCliente"></param>
        /// <param name="toArticulo"></param>
        /// <param name="tdFecha"></param>
        /// <param name="tdtResposta"></param>
        /// <param name="tdtRegalo"></param>
        /// <param name="tcDivisa"></param>
        /// <param name="tnUnidades"></param>
        /// <param name="tcObra"></param>
        /// <param name="tcTalla"></param>
        /// <param name="tcColor"></param>
        /// <param name="tcTarifaAlbaran"></param>
        /// <param name="tlEsCaja"></param>
        /// <param name="tnCajasReales"></param>
        /// <param name="tdtCurTc"></param>
        ///  <param name="tcCodigoAgrupacion"></param>
        /// <returns></returns>
        public virtual int _ObtenerPrecio(Cliente toCliente, Articulo toArticulo, DateTime tdFecha, ref DataTable tdtResposta, ref DataTable tdtRegalo,
                      string tcDivisa, decimal tnUnidades = 0.0M, string tcObra = "",
                      string tcTalla = "", string tcColor = "", string tcTarifaAlbaran = "", bool tlEsCaja = false,
                      decimal tnCajasReales = 0.0M, DataTable tdtCurTc = null, string tcCodigoAgrupacion = "")
        {
            return EW_PRECIOS._PrecioVenta(toCliente, toArticulo, tdFecha, ref tdtResposta, ref tdtRegalo, tcDivisa, tnUnidades, tcObra,
                                tcTalla, tcColor, tcTarifaAlbaran, tlEsCaja, tnCajasReales, tdtCurTc, tcCodigoAgrupacion);
        }

        // EMULACIÓN DE DATATABLE PARA LOS LOTES
        public virtual DataTable _Stocklote_AsignacionVentas(Lote toLote, dynamic toLinia, string tslote)
        {
            string lsWhere = "";

            lsWhere = " and lt.articulo ='" + toLinia._Articulo + "' " +
                        " and lt.lote='" + tslote + "' " +
                        " and lt.almacen='" + toLinia._Almacen + "' ";

            DataTable ldStockLote = toLote._Stocklote(lsWhere);

            if (ldStockLote.Rows.Count > 0)
            {

                if (!ldStockLote.Columns.Contains("UNIASIG"))
                    ldStockLote.Columns.Add("UNIASIG", typeof(decimal));

                if (!ldStockLote.Columns.Contains("PESASIG"))
                    ldStockLote.Columns.Add("PESASIG", typeof(decimal));

                if (!ldStockLote.Columns.Contains("UBICA"))
                    ldStockLote.Columns.Add("UBICA", typeof(string));

                if (!ldStockLote.Columns.Contains("ASI"))
                    ldStockLote.Columns.Add("ASI", typeof(string));

                if (!ldStockLote.Columns.Contains("SEL"))
                    ldStockLote.Columns.Add("SEl", typeof(bool));
            }

            ldStockLote.AcceptChanges();

            return ldStockLote;
        }

    }
}
