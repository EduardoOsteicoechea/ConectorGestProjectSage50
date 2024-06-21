using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.ComponentModel;
using System.Reflection;

//Sage50
using sage.ew.ewbase;
using sage.ew.cliente;
using sage.ew.articulo;
using sage.ew.interficies;
using sage.ew.enumerations;
using sage.ew.docscompra;
using sage.ew.global;
using sage.ew.db;
using sage.ew.netvfp;
using sage.ew.objetos;
using sage.ew.objetos.UserControls;
using sage.ew.lote;
using sage.ew.lote.Clases;
using sage.ew.lote.UserControls;
using sage.ew.serie;
using sage.ew.serie.UserControls;
using sage.ew.functions;

// propio
using ExampleSatelite.Sage50.Datos;
using sage.ew.docventatpv;                      // para usar los lotes

namespace ExampleSatelite.Sage50.Negocio
{
    public class BuyDocument
    {
    }

    public class PedidoCompra : BaseDocument
    {
        ewDocCompraPEDIDO _oDocCompraPED;
        ewDocCompraLinPEDIDO _oLinia;
        ewDocCompraALBARAN _oDocCompra;


        public Boolean _to_AlbaranVenta(clsPedicom toPedicom, string tsAlbaranNumero, DateTime? tdAlbaraFecha)
        {
            this._Error_Message = string.Empty;
            bool llOk = false, llTraspasar = true;

            if (toPedicom.Cabecera != null)
            {
                _oDocCompraPED = new ewDocCompraPEDIDO();
                if (_oDocCompraPED._Existe(toPedicom.Cabecera.empresa, toPedicom.Cabecera.numero, toPedicom.Cabecera.proveedor))
                {
                    _oDocCompraPED._Load(toPedicom.Cabecera.empresa, toPedicom.Cabecera.numero, toPedicom.Cabecera.proveedor);


                    DateTime ldAlbaranFecha = DateTime.Now;
                    if (tdAlbaraFecha != null)
                        ldAlbaranFecha = (DateTime)tdAlbaraFecha;

                    List<clsPedicomLineas> LineasTraspaso = new List<clsPedicomLineas>();

                    // recorremos las lineas del pedido de venta
                    foreach (ewDocCompraLinPEDIDO loLinea in _oDocCompraPED._Lineas)
                    {

                        if (toPedicom.Lineas.Any())
                        {
                            clsPedicomLineas loLinea2 = toPedicom.Lineas.Find(x => x.linea == loLinea._Linea);
                            if (loLinea2 != null)
                            {
                                if (loLinea._Articulo.Trim() == loLinea2.articulo.Trim())
                                {
                                    if ((loLinea._Unidades - loLinea._Servidas) > 0)
                                    {
                                        loLinea._TraspasarLinea = true;
                                        loLinea._Traspaso = (loLinea._Unidades - loLinea._Servidas);
                                        loLinea._Traspasar_Unidades = (loLinea._Unidades - loLinea._Servidas);
                                        loLinea._Traspasar_Cajas = 0;
                                        loLinea._Traspasar_Peso = 0;
                                        //loLinea._Traspasar_Extensiones = _Obtener_Datos_Extensiones(loLinea)
                                    }
                                }
                                else
                                {
                                    this._Error_Message += "No se puede traspasar el pedido.\r\n" +
                                                           "El artículo" + loLinea._Articulo + " no aparece en la linea " + loLinea2.linea.ToString().Trim() + ".\r\n";
                                    llTraspasar = false;
                                    break;
                                }
                            }
                            else
                            {
                                loLinea._TraspasarLinea = true;
                                loLinea._Traspaso = loLinea._Unidades;
                                loLinea._Traspasar_Unidades = loLinea._Unidades;
                                loLinea._Traspasar_Cajas = loLinea._Cajas;
                                loLinea._Traspasar_Peso = loLinea._Peso;
                                //loLinea._Traspasar_Extensiones = _Obtener_Datos_Extensiones(loLinea)
                            }
                        }
                    }

                    if (llTraspasar)
                    {
                        // creamos el objeto para el albarán de compra
                        _oDocCompra = new ewDocCompraALBARAN();

                        // Diccionario de parametros que se envian para la generación del traspaso a la clase del CORE del Sage50
                        Dictionary<string, object> loDicParam = new Dictionary<string, object>();
                        loDicParam.Add("tipodoc", 2);           // 2 Albarán de venta
                        loDicParam.Add("fecha", ldAlbaranFecha);
                        loDicParam.Add("almacen", this._Almacen);
                        loDicParam.Add("divisa", _oDocCompraPED._Divisa);
                        loDicParam.Add("pedidotoalbaran", true);
                        loDicParam.Add("traspasoportes", true);
                        loDicParam.Add("portes", _oDocCompraPED._Pie._Portes);

                        if (!string.IsNullOrWhiteSpace(tsAlbaranNumero) && _oDocCompra._Existe(toPedicom.Cabecera.empresa, tsAlbaranNumero, toPedicom.Cabecera.proveedor))
                        {
                            _oDocCompra._Load(toPedicom.Cabecera.empresa, tsAlbaranNumero, toPedicom.Cabecera.proveedor);
                            if (!_oDocCompra._Solo_Lectura)
                            {
                                if (_oDocCompra._Cabecera._Proveedor == _oDocCompraPED._Cabecera._Proveedor)
                                    loDicParam.Add("docudestino", _oDocCompra);
                                else
                                {
                                    this._Error_Message += "No se puede traspasar el pedido.\r\n" +
                                                            "El cliente del pedido de venta es diferente al cliente del albarán de venta.\r\n";
                                    llTraspasar = false;
                                }
                            }
                            else
                            {
                                this._Error_Message += "No se puede traspasar el pedido.\r\n" +
                                                       "El albarán de venta es de solo lectura.\r\n";
                                llTraspasar = false;
                            }
                        }

                        if (llTraspasar)
                        {
                            string lcNumero = _oDocCompraPED._To_Albaran(loDicParam);

                            if (_oDocCompraPED._Documento_Traspasado_Totalmente())
                                _oDocCompraPED._Cabecera._Traspasado = true;

                            _oDocCompraPED._Desbloquear_Documento();

                            if (string.IsNullOrEmpty(_oDocCompraPED._Mensaje_Error))
                            {
                                llOk = string.IsNullOrEmpty(lcNumero.Trim()) ? true : false;
                            }
                            else
                            {
                                this._Error_Message += _oDocCompraPED._Mensaje_Error + ".\r\n";
                            }
                        }
                    }
                }
                else
                {
                    this._Error_Message += "El pedido de compra no existe.\r\n";
                }
            }
            else
            {
                this._Error_Message += "Los datos de la cabecera pedido son obligatorios.\r\n";
            }

            return llOk;
        }

    }

    public class AlbaranCompra : BaseDocument
    {
        ewDocCompraALBARAN _oDocCompra;
        ewDocCompraLinALBARAN _oLinia;
        List<LoteDocCompra> _oLinComDetLotes;

        public Boolean _Create(clsAlbacom toAlbacom)
        {
            this._Error_Message = string.Empty;
            bool llOk = false, llContinue = false;

            if (toAlbacom.Cabecera != null && toAlbacom.Lineas != null && toAlbacom.Lineas.Count != 0)
            {
                Proveedor loProveedor;

                _oDocCompra = new ewDocCompraALBARAN();
                _oLinia = new ewDocCompraLinALBARAN();

                toAlbacom.Cabecera.empresa = this._Empresa;
                toAlbacom.Cabecera.ejercicio = this._Ejercicio;
                //toAlbacom.Cabecera.letra = toAlbacom.Cabecera.letra.Trim().PadLeft(2, ' ');
                toAlbacom.Cabecera.numero = toAlbacom.Cabecera.numero.Trim().PadLeft(10, ' ');


                // Abrimos el objeto de cliente
                loProveedor = new Proveedor();
                loProveedor._Codigo = toAlbacom.Cabecera.proveedor;

                // comprobamos que exista el cliente para poder crear el pedido
                if (loProveedor._Existe_Registro())
                {
                    // comprobamos si el pedido ya existe
                    if (_oDocCompra._Existe(toAlbacom.Cabecera.empresa, toAlbacom.Cabecera.numero, toAlbacom.Cabecera.proveedor))
                    {
                        // si ya existe, lo cargamos
                        _oDocCompra._Load(toAlbacom.Cabecera.empresa, toAlbacom.Cabecera.numero, toAlbacom.Cabecera.proveedor);
                        if (_oDocCompra._Cabecera._Proveedor!= toAlbacom.Cabecera.proveedor)
                            this._Error_Message += "El Código del cliente del pedido es " + _oDocCompra._Cabecera._Proveedor + ", y se esta informando un cliente es diferente " + toAlbacom.Cabecera.proveedor + "\r\n";
                        else
                            llContinue = true;
                    }
                    else
                    {
                        // no existe, lo creamos
                        _oDocCompra._New(toAlbacom.Cabecera.empresa, toAlbacom.Cabecera.numero, toAlbacom.Cabecera.proveedor);
                        _oDocCompra._Cabecera._Proveedor = toAlbacom.Cabecera.proveedor;
                        llContinue = true;
                    }

                    if (llContinue)
                    {
                        // validamos la forma de pago
                        string lsFPago = ReplicatedDB.SQLValor("FPAG", "CODIGO", toAlbacom.Cabecera.formapago, "CODIGO").ToString();
                        if (!string.IsNullOrEmpty(lsFPago))
                            _oDocCompra._Cabecera._FormaPago = lsFPago;

                        if (!string.IsNullOrEmpty(toAlbacom.Cabecera.observaciones))
                            _oDocCompra._Cabecera._Observacio = toAlbacom.Cabecera.observaciones;

                      
                        if (toAlbacom.Lineas != null && toAlbacom.Lineas.Count != 0)
                        {
                            string lsCodigo = string.Empty;

                            foreach (var LineaAlbaran in toAlbacom.Lineas)
                            {
                                _oLinia = _oDocCompra._AddLinea();

                                lsCodigo = ReplicatedDB.SQLValor("ARTICULO", "CODIGO", LineaAlbaran.articulo, "CODIGO").ToString();
                                if (!string.IsNullOrEmpty(lsCodigo))
                                {
                                    _oLinia._Articulo = lsCodigo;
                                    _oLinia._Talla = LineaAlbaran.talla;
                                    _oLinia._Color = LineaAlbaran.color;
                                    //_oLinia._Peso = LineaPedido.peso;
                                    //_oLinia._Cajas = LineaPedido.cajas;

                                    if (LineaAlbaran.lotes == null)
                                        _oLinia._Unidades = LineaAlbaran.unidades;



                                    #region Lotes

                                    // pasamos los lotes de la linea actual
                                    if (LineaAlbaran.lotes != null)
                                    {

                                        // creamos el objeto para los lotes de la linea del documento
                                        _oLinComDetLotes = new List<LoteDocCompra>();

                                        // Diccionario de valores para hacer el _UpdateSilent
                                        // y asi no ejecutar el set de _unidades
                                        Dictionary<String, Object> ldicValores = new Dictionary<string, object>();
                                        ewCampo loCampoUnidades = new ewCampo();
                                        loCampoUnidades._OldVal = loCampoUnidades._NewVal = LineaAlbaran.unidades;
                                        ldicValores.Add("_nUnidades", loCampoUnidades);
                                        _oLinia._UpdateSilent(ldicValores);

                                        // pasamos los lotes para el albaran de venta
                                        foreach (clsAlbacomLineasLotes loItem in LineaAlbaran.lotes)
                                        {
                                            //DataRow loRow = null;

                                            LoteDocCompra loteCompra = new LoteDocCompra(loItem.lote, _oLinia);
                                            //loteCompra._Add(lsCodigo, LineaAlbaran.unidades, LineaAlbaran.talla, LineaAlbaran.color, LineaAlbaran.cajas, LineaAlbaran.peso);

                                            loteCompra._Almacen = toAlbacom.Cabecera.almacen;
                                            loteCompra._Articulo = lsCodigo;
                                            loteCompra._Talla = LineaAlbaran.talla;
                                            loteCompra._Color = LineaAlbaran.color;
                                            loteCompra._Lote = loItem.lote;
                                            loteCompra._Unidades = loItem.unidades;
                                            loteCompra._Peso = loItem.peso;
                                            //loteCompra._Ubicacion = "";
                                            //loteCompra._Caducidad = null;

                                            _oLinComDetLotes.Add(loteCompra);


                                        }

                                    }

                                    #endregion Lotes

                                    #region series

                                    //if (LineaAlbaran.series != null)
                                    //{
                                    //    LinVenDet<LinVenDetSeries> loLinVenDetSer = new LinVenDet<LinVenDetSeries>();
                                    //    loLinVenDetSer._Lineas = _oLinia;
                                    //    loLinVenDetSer._Automatico = false;
                                    //    loLinVenDetSer._Load();

                                    //    // pasamos las series para el albaran de venta
                                    //    foreach (clsAlbavenLineasSeries loItem in LineaAlbaran.series)
                                    //    {
                                    //        LinVenDetSeries loSerie = new LinVenDetSeries();
                                    //        loSerie._Codigo = loItem.serie;
                                    //        loSerie._Unidades = loItem.unidades;
                                    //        loSerie._Peso = loItem.peso;

                                    //        loLinVenDetSer._lisCodigos.Add(loSerie);

                                    //    }

                                    //}

                                    #endregion series



                                    // si indicamos que aplique los precios que lee desde el objeto
                                    // de lo contrario, aplicará los precios calculados por Sage50
                                    toAlbacom.Cabecera.precios = true;

                                    if (toAlbacom.Cabecera.precios)
                                    {

                                        #region Tratamiento precios de ejemplo IVA/DIVISA
                                        ///
                                        /// Cuando el documento trabaja con IVA incluido y/o se trabaja con una divisa diferente
                                        /// al de la empresa
                                        ///
                                        //if (_oDocVenta._Cabecera._IvaInc)
                                        //{
                                        //    if (_oDocVenta._Cabecera._Divisa != EW_GLOBAL._Moneda._Codigo)
                                        //    {
                                        //        _oLinia._PrecioDivisaIva = lnPrecio;
                                        //    }
                                        //    else
                                        //    {
                                        //        _oLinia._PrecioIva = lnPrecio;
                                        //    }
                                        //}
                                        //else
                                        //{
                                        //    if (_oDocVenta._Cabecera._Divisa != EW_GLOBAL._Moneda._Codigo)
                                        //    {
                                        //        _oLinia._PrecioDivisa = lnPrecio;
                                        //    }
                                        //    else
                                        //    {
                                        //        _oLinia._Precio = lnPrecio;
                                        //    }
                                        //}
                                        #endregion Tratamiento precios de ejemplo IVA/DIVISA

                                        _oLinia._Precio = LineaAlbaran.precio;
                                        _oLinia._Dto1 = LineaAlbaran.dto1;
                                        _oLinia._Dto2 = LineaAlbaran.dto2;
                                        _oLinia._Recalcular_Importe();
                                    }

                                }
                                else
                                {
                                    // Es una linea de comentario
                                    if (!string.IsNullOrEmpty(LineaAlbaran.definicion))
                                    {
                                        _oLinia._Definicion = LineaAlbaran.definicion;
                                        _oLinia._TipoIva = "";
                                    }
                                    else
                                    {
                                        this._Error_Message += "El código de articulo " + LineaAlbaran.articulo + ", no existe\r\n";
                                    }


                                }


                                if (_oLinia._Save())
                                {
                                    if (_oLinComDetLotes != null && _oLinComDetLotes.Count > 0)
                                    {
                                        foreach (LoteDocCompra loteCompra in _oLinComDetLotes)
                                            loteCompra._Asignar_Articulos_Lote(_oLinia, false);
                                    }
                                }

                                _oLinComDetLotes = null;
                            }

                        }

                        // grabamos el pedido
                        _oDocCompra._Totalizar();
                        llOk = _oDocCompra._Save();

                        if (llOk)
                        {
                            toAlbacom.Cabecera.numero = _oDocCompra._Numero;
                            toAlbacom.Cabecera.factura = _oDocCompra._Cabecera._Factura;
                        }
                        else
                        {
                            string lsNumero = toAlbacom.Cabecera.numero;
                            this._Error_Message += "No se ha podido guardar el albarán de compra :" + lsNumero + "\r\n";
                        }
                    }
                }
                else
                {
                    this._Error_Message += "El Código de proveedor " + toAlbacom.Cabecera.proveedor + ", no existe\r\n";
                }
            }
            else
            {
                if (toAlbacom.Cabecera == null)
                    this._Error_Message += "Los datos de la cabecera albarán son obligatorios\r\n";
                else
                    this._Error_Message += "Es obligatorio insertar un artículo para poder generar el albarán\r\n";
            }

            return llOk;
        }


        // EMULACIÓN DE DATATABLE PARA LOS LOTES
        private DataTable _Stocklote_AsignacionCompras(Lote toLote, dynamic toLinia, string tslote)
        {
            string lsWhere = "";

            lsWhere = " and lt.articulo ='" + toLinia._Articulo + "' " +
                        " and lt.lote='" + tslote + "' " +
                        " and lt.almacen='" + toLinia._Almacen + "' ";
            DataTable ldStockLote = toLote._Stocklote(lsWhere);

            //if (ldStockLote.Rows.Count > 0)
            //{

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
            //}

            ldStockLote.AcceptChanges();

            return ldStockLote;
        }
    }


}
