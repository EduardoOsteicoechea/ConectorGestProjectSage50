using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Windows.Forms;

// Sage50
using sage.ew.global;
using sage.ew.cliente;
using sage.ew.docscompra;
using sage.ew.contabilidad;
using sage.ew.functions;
using sage.ew.interficies;
using sage.ew.docsven;
using sage.ew.global.Diccionarios;

namespace ExampleSatelite.Sage50.Negocio
{
    public class EjemploAsientos
    {
        private string _Error_Message = "";
        private int _nDigitos = Convert.ToInt32(EW_GLOBAL._GetLenCampo(KeyDiccionarioLenCampos.wn_digitos));
        private string _Ejercicio = EW_GLOBAL._GetVariable("wc_any").ToString();


        public string _Error()
        {
            return this._Error_Message.Trim();
        }

        #region Ventas

        /// <summary>
        /// Facturar Albaranes de ventas
        /// </summary>
        public void GenerarFacturasVentasAlbaranes()
        {
            // Configuración de parámetros para la generación de la factura de venta.
            //
            ParamGenFactAlbaranesVenta loCfg = new ParamGenFactAlbaranesVenta();

            loCfg._TipoFactura = 0;                                     // Tipo factura (0 normal, 1 rectifictiva, 2 tickets)
            loCfg._FechaFactura = DateTime.Today;                       // Fecha de factura (la fecha de asiento será la fecha de factura).
            loCfg._Factura = "  RRRFFFR1";                              // Podemos declarar un nº de factura concreto. Si queremos que utilize uno nuevo por contador dejar esta propiedad en blanco.

            // si tiene albaranes
            loCfg._ListaAlbaranes.Add("518".PadLeft(10) + "SF");        // Declarar los albaranes a facturar en esta lista en formato string NUMERO+LETRA
            loCfg._ListaAlbaranes.Add("519".PadLeft(10) + "SF");
            //

            loCfg._PresentarEntregasACuentaCliente = false;             // Presentar formulario entregas a cuenta clientes si/no. Si no se presenta este formulario se imputan automáticamente las entregas que haya.

            loCfg._PresentarVencimientos = true;                        // Presentar formulario vencimientos por pantalla si/no.

            loCfg._ContabilizarCobro = true;                            // Contabilizar cobro si/no.
            loCfg._PresentarContabilizarCobro = true;                   // Caso de contabilizar cobro si, si se pide al usuario fecha y cuenta tesoreria, si no se pregunta se utilizan los valores pasados en las dos siguientes propiedades.
            loCfg._ContabilizarCobroCuenta = "5720000003";              // Cuenta tesoreria para el cobro.
            loCfg._ContabilizarCobroFecha = new DateTime(2018, 6, 22);  // Fecha para el cobro.

            loCfg._PresentarAsiento = true;                             // Presentar asiento generado por pantalla si/no.

            loCfg._Cambio = 1;                                          // Tasa de cambio a utilizar en la factura, caso de facturar en divisa. La divisa se asume la del primer albarán de la lista.


            // Llamada a la generación de la factura transferiendo la configuración del proceso. Esta llamada hace todo.
            //
            bool llOk = CONTABILIDAD._GenerarFacturaVentaAlbaranes(loCfg);


            // Control de errores.
            //
            if (!llOk)
            {
                if (!string.IsNullOrWhiteSpace(CONTABILIDAD._Error_Message))
                    MessageBox.Show(CONTABILIDAD._Error_Message);
                else
                    MessageBox.Show("Error en la generación de la factura.");
            }
            else
            {
                // Acceso al nº de factura generado.
                //
                string lcFactura = loCfg._Factura;

                // Acceso al asiento de factura generado.
                //
                IAsientos loAsiento = loCfg._AsientoGenerado;
            }

        }

        /// <summary>
        /// Generar Factura de venta sin albarán
        /// </summary>
        public void GenerarFacturasVentas()
        {
            bool llretencion = false;
            bool llSerFact = true;

            EW_GLOBAL._GetAllVariable();
            
            ParamGenFactVenta toCfgGenFactVenta = new ParamGenFactVenta();
            toCfgGenFactVenta._Cliente = "43000002";
            
            toCfgGenFactVenta._SerieFra = "";

            // Si trabaja con series en facturas
            if (llSerFact)
                toCfgGenFactVenta._SerieFra = "SX";

            toCfgGenFactVenta._Factura = "SX       1";
            toCfgGenFactVenta._Factura = toCfgGenFactVenta._Factura.Trim().PadLeft(10, ' ');

            toCfgGenFactVenta._TipoFactura = 0;
            toCfgGenFactVenta._FechaAsiento = DateTime.Now;
            toCfgGenFactVenta._FechaFactura = DateTime.Now;
            toCfgGenFactVenta._PresentarVencimientos = false;
            toCfgGenFactVenta._ContabilizarCobro = false;
            toCfgGenFactVenta._PresentarAsiento = false;

            #region IVA
            // Codigo Iva, Base Imponible
            toCfgGenFactVenta._AnadirTipoIva("03", 1000);
            toCfgGenFactVenta._AnadirTipoIva("04", 500);
            #endregion IVA

            #region Contrapartida
            // Codigo Contrapartida, Base Imponible/Importe, suplido true/false
            toCfgGenFactVenta._AnadirContrapartida("70000001", 1500, false);
            #endregion Contrapartida

            toCfgGenFactVenta._DefinicionDebe = "";
            toCfgGenFactVenta._DefinicionHaber = "";

            toCfgGenFactVenta._Divisa = "000";
            toCfgGenFactVenta._Cambio = 1;
            // % pronto pago
            toCfgGenFactVenta._PrcDtoPP = 0;

            // %recargo financiero
            toCfgGenFactVenta._PrcRecFinan = 0;

            toCfgGenFactVenta._Recc = false;
            
            // Si los importes de tipos de IVA se han declarado IVA INCLUIDO o SIN IVA.
            toCfgGenFactVenta._IvaIncluido = false;
            
            // recargo equivalencia en función de configuración en ficha cliente.
            toCfgGenFactVenta._RecEquiv = false;

            ///
            if (llretencion)
            {
                RetencionGenFact cfgRetencion = toCfgGenFactVenta._ConfiguracionParaRetencion.Value;

                cfgRetencion._Retencion_Codigo = "01";
                cfgRetencion._Retencion_Cuenta = ""; // Cuenta contable para la retención
                cfgRetencion._RetencionSobreBase = false;
                cfgRetencion._RetencionSobreTotal = true;
                cfgRetencion._PrcRetencion = 5;
            }

            AsientosFacturasVentaGenerador loGenAsiFacVen = new AsientosFacturasVentaGenerador();
            bool llOk = loGenAsiFacVen._GenerarFacturaRapida(toCfgGenFactVenta);

            //bool llOk = CONTABILIDAD._GenerarFacturaVenta(toCfgGenFactVenta);
            
            if (!llOk)
                _Error_Message = loGenAsiFacVen._Error_Message.Trim();

        }

        /// <summary>
        /// Cobro de facturas de Ventas
        /// </summary>
        public void GenerarCobroFactura()
        {
            EW_GLOBAL._GetAllVariable();

            int _Pendiente = 0;
            decimal _Importe = 100, _EntregaDiv = 100;

            AsientosCobrosGeneradorLinea loNuevaLinea = null;

            AsientosCobrosGenerador loCobro = new AsientosCobrosGenerador();
            loCobro._LinkForm = false;
            loCobro._Fecha = DateTime.Now;
            loCobro._Cuenta = "57200001";
            loCobro._Divisa = "000";
            loCobro._Cambio = 1;
            loCobro._Definicion = "COBRO FACTURA ";
            loCobro._AsientoPorLinea = false;
            // aqui se agregan las lineas de pevisiones
            //loCobro._Detalle.Add();

            loNuevaLinea = new AsientosCobrosGeneradorLinea(loCobro);

            loNuevaLinea._Automatico = true;
            loNuevaLinea._Cuenta = "43000002";                                      // Cliente
            loNuevaLinea._DefinicionAsiento = "COBRO FRA. SX1/1";              // _Factura.Trim() + "/" + _Orden.ToString();

            loNuevaLinea._Factura = "SX       1";
            loNuevaLinea._Orden_Numereb = 1;
            loNuevaLinea._Entrega = _EntregaDiv;
            loNuevaLinea._Importe = _Importe;
            loNuevaLinea._Emision = DateTime.Now;
            loNuevaLinea._Prevision = true;
            loNuevaLinea._Periodo = Convert.ToInt32(_Ejercicio);
            loNuevaLinea._Pendiente = _Pendiente;
            loNuevaLinea._Vencimiento = DateTime.Now.AddDays(30);

            loNuevaLinea._Divisa = "000";
            loNuevaLinea._CambioPrevision = 1;

            loCobro._Detalle.Add(loNuevaLinea);

            loCobro._PresentarAsiento = false;
            bool llOk = loCobro._Generar_Asiento();
            if (!llOk)
                _Error_Message = loCobro._Error_Message.Trim();

        }

        #endregion Ventas

        #region Compras
        /// <summary>
        /// Facturar Albaranes de compras
        /// </summary>
        public void GenerarFacturasComprasAlbaranes()
        {
            // Configuración de parámetros para la generación de la factura de compra.
            //
            ParamGenFactAlbaranesCompra loCfg = new ParamGenFactAlbaranesCompra();

            loCfg._Factura = "RRRFFF".PadLeft(20);                   // Podemos declarar un nº de factura o dejarlo en blanco si está configurado trabajar con contador de facturas de compra.

            loCfg._PresentarFechasAsiFacGenVenc = false;             // Si debe mostrar formulario solicitando fecha factura, fecha asiento, tasa de cambio y tipo de fecha origen para creación de vencimientos.
                                                                     // Si no se muestra este formulario deberán suministrase estos datos en las propiedades respectivas que siguen.

            loCfg._FechaAsiento = new DateTime(2018, 6, 14);         // Fecha de asiento.
            loCfg._FechaFactura = new DateTime(2018, 6, 13);         // Fecha de factura.
            loCfg._Cambio = 1;                                       // Tasa de cambio para la factura, caso facturas en divisa. Se asume la divisa del primer albarán de la lista.
            loCfg._FechaOrigenGenVencim = AsientosFacturasCompraGenerador.FechaOrigenGenVencim.FechaAsiento;    // Tipo fecha origen para creación de vencimiento (fecha asiento, fecha factura, fecha operación).

            loCfg._ListaAlbaranes.Add("4000000001" + "1818".PadLeft(10));   // Lista albaranes de compra a incluir en la factura en formato string PROVEEDOR+NUMERO
            loCfg._ListaAlbaranes.Add("4000000001" + "2020".PadLeft(10));   // El proveedor se deduce automáticamente de los albaranes, debe ser el mismo en todos ellos.


            loCfg._PresentarCuadrarFacturaCompra = false;            // Presentar si/no formulario para cuadrar factura de compra. Solo tiene efecto si está el opcflag activado "Cuadrar factura de compra".

            loCfg._PresentarFacturaCEE = false;                      // Presentar si/no formulario solicitud datos factura CEE, solo para facturas que generen intracomunitario.
                                                                     // Si no se muestra este formulario se puede suministrar estos datos en las propiedades respectivas que siguen, y si no se especifican
                                                                     // estas propiedades se utilizarán los valores por defecto que se muestran en la pantalla al entrar en ella (cuando se muestra).

            loCfg._FacturaCEE = 1111;                                // Nº factura CEE.
            loCfg._Fecha_FacturaCEE = new DateTime(2018, 12, 12);    // Fecha factura CEE.
            loCfg._ConceptoFacturaCEE = "Concepto factura CEE";      // Concepto factura CEE.

            loCfg._PresentarEntregasACuentaProveedor = false;        // Presentar si/no formulario entregas a cuenta proveedor. Si no se presenta el formulario se imputan todas las entregas que permita la factura.

            loCfg._PresentarVencimientos = false;                    // Presentar formulario vencimientos por pantalla si/no.

            loCfg._PresentarAsiento = true;                          // Presentar asiento generado por pantalla si/no



            // Llamada a la generación de la factura transferiendo la configuración del proceso. Esta llamada hace todo.
            //
            bool llOk = CONTABILIDAD._GenerarFacturaCompraAlbaranes(loCfg);



            // Control de errores.
            //
            if (!llOk)
            {
                if (!string.IsNullOrWhiteSpace(CONTABILIDAD._Error_Message))
                    MessageBox.Show(CONTABILIDAD._Error_Message);
                else
                    MessageBox.Show("Error en la generación de la factura.");
            }
            else
            {
                // Acceso al nº de factura generado, caso de no pasar inicialmente nº de facturar y generarse por contador.
                //
                string lcFactura = loCfg._Factura;

                // Acceso al asiento de factura generado.
                //
                IAsientos loAsiento = loCfg._AsientoGenerado;
            }

        }

        /// <summary>
        /// Factura de compra sin albarán
        /// </summary>
        public void GenerarFacturasCompras()
        {
            int _LongFactCompra = Convert.ToInt32(EW_GLOBAL._GetLenCampo(KeyDiccionarioLenCampos.wn_factcompra));

            EW_GLOBAL._GetAllVariable();

            ParamGenFactCompra toCfgGenFactCompra = new ParamGenFactCompra();
            toCfgGenFactCompra._Proveedor = "40000001";
            toCfgGenFactCompra._FechaAsiento = DateTime.Now;
            toCfgGenFactCompra._FechaFactura = DateTime.Now;

            toCfgGenFactCompra._Factura = "FCXYZ1000";
            toCfgGenFactCompra._Factura = toCfgGenFactCompra._Factura.Trim().PadLeft(_LongFactCompra, ' '); 
            toCfgGenFactCompra._Divisa = "000";
            toCfgGenFactCompra._Cambio = 1;

            toCfgGenFactCompra._IvaIncluido = false;

            #region IVA
            // Codigo Iva, Base Imponible
            toCfgGenFactCompra._AnadirTipoIva("03", 1000);
            toCfgGenFactCompra._AnadirTipoIva("02", 500);
            #endregion IVA

            #region Contrapartida
            // Codigo Contrapartida, Base Imponible/Importe, suplido true/false
            toCfgGenFactCompra._AnadirContrapartida("60000001", 1500, false);
            #endregion Contrapartida

            toCfgGenFactCompra._DefinicionDebe = "";
            toCfgGenFactCompra._DefinicionHaber = "";

            toCfgGenFactCompra._PresentarVencimientos = false;
            toCfgGenFactCompra._ContabilizarPago = false;
            toCfgGenFactCompra._PresentarFechaBancoPago = false;
            toCfgGenFactCompra._PresentarAsiento = false;

            toCfgGenFactCompra._CuentaBancoPago = "";
            toCfgGenFactCompra._FechaPago = null;
            toCfgGenFactCompra._PrcDtoPP = 0.0M;
            toCfgGenFactCompra._AplicaRetPro = false;


            AsientosFacturasCompraGenerador loGenAsiFacComp = new AsientosFacturasCompraGenerador();
            bool llOk = loGenAsiFacComp._GenerarFacturaRapida(toCfgGenFactCompra);
            //bool llOk = CONTABILIDAD._GenerarFacturaCompra(toCfgGenFactCompra);
            if (llOk)
            {
                //IAsientos loAsiento = loCfgFactCompra._AsientoGenerado;
                ////Marcar asiento como Capture y con el tipo_mov correspondiente
                //loAsiento._Capture = true;
                //loAsiento._Tipo_mov = Convert.ToInt32(SageCapture.TipoTransac.FacturaCompra); //Facturas de compra

                //loAsiento._Save();
            }
            else
            {
                _Error_Message = loGenAsiFacComp._Error_Message.Trim();
            }

        }

        /// <summary>
        /// Pago de facturas de compras
        /// </summary>
        public void GenerarPagoFactura()
        {
            EW_GLOBAL._GetAllVariable();
            string lsFactura = "";
            int _Pendiente = 0;
            decimal _Importe = 1000M, _EntregaDiv = 1000M;
            int _LongFactCompra = Convert.ToInt32(EW_GLOBAL._GetLenCampo(KeyDiccionarioLenCampos.wn_factcompra));

            AsientosPagosGeneradorLinea loNuevaLinea = null;

            AsientosPagosGenerador loPago = new AsientosPagosGenerador();
            loPago._LinkForm = false;
            loPago._Fecha = DateTime.Now;
            loPago._Cuenta = "57200001";
            loPago._Divisa = "000";
            loPago._Cambio = 1;
            loPago._Definicion = "PAGO FACTURA ";
            loPago._AsientoPorLinea = false;
            // aqui se agregan las lineas de pevisiones
            //loCobro._Detalle.Add();

            loNuevaLinea = new AsientosPagosGeneradorLinea(loPago);
            
            lsFactura = "FCXYZ1000";

            loNuevaLinea._Automatico = true;
            loNuevaLinea._Cuenta = "40000001";                                                  // Proveedor
            loNuevaLinea._DefinicionAsiento = "PAGO FRA. "+ lsFactura+ "/1";                    // _Factura.Trim() + "/" + _Orden.ToString();
            
            loNuevaLinea._Factura = lsFactura.Trim().PadLeft(_LongFactCompra, ' '); ;
            loNuevaLinea._Orden_Numereb = 1;
            loNuevaLinea._Entrega = _EntregaDiv;
            loNuevaLinea._Importe = _Importe;
            loNuevaLinea._Emision = DateTime.Now;
            loNuevaLinea._Prevision = true;
            loNuevaLinea._Periodo = Convert.ToInt32(_Ejercicio);
            loNuevaLinea._Pendiente = _Pendiente;
            loNuevaLinea._Vencimiento = DateTime.Now.AddDays(30);

            loNuevaLinea._Divisa = "000";
            loNuevaLinea._CambioPrevision = 1;

            loPago._Detalle.Add(loNuevaLinea);

            loPago._PresentarAsiento = false;
            bool llOk = loPago._Generar_Asiento();
            if (!llOk)
                _Error_Message = loPago._Error_Message.Trim();
        }

        #endregion Compras

        #region Asiento Contables sin documentos

        public void GenerarAsientos()
        {

        }

        // Creación de asientos en Sage50
        // desde un DataTable, no genera previsiones de cobros, ni pagos
        public String GenerarAsientoSage50(DataTable toAsiento, Excepcion toExcepcion)
        {

            String lcGuid = "";
            String lcReferencia, lcCuenta, lcCodigo;
            Asientos loAsiento = new Asientos();
            Int16 lnRow = 0;
            AsientosLinea loAsiLin;
            Cliente loCliente = new Cliente();
            Proveedor loProveedor = new Proveedor();
            Boolean lbEsCliente = false;
            Boolean lbEsProveedor = false;

            loAsiento._New();

            foreach (DataRow loRow in toAsiento.Rows)
            {
                lnRow++;
                lbEsCliente = false;
                lbEsProveedor = false;

                // wpm.. ojo
                loAsiLin = (AsientosLinea)loAsiento._AddLinea();
                //loAsiLin = new AsientosLinea();

                if (lnRow == 1)
                {
                    loAsiento._Fecha = Convert.ToDateTime(loRow["fecha"]);
                    loAsiento._Tipo = loRow["tipodoc"].ToString();
                    loAsiento._Factura = loRow["factura"].ToString();
                    lcReferencia = loRow["referencia"].ToString().Trim();
                    if (!String.IsNullOrEmpty(lcReferencia))
                        loAsiento._Referencia = lcReferencia;
                    else
                        loAsiento._Referencia = "Nº asiento: " + loRow["numero"].ToString().Trim();

                    // asignamos el tipo de operación a todo el asiento
                    loAsiento._Operacion = (int)loRow["operacion"];

                    loAsiento._Archivo = "";
                }

                lcCuenta = loRow["cuenta"].ToString().Trim();
                loAsiLin._Cuenta = lcCuenta;
                loAsiLin._Definicion = loRow["definicion"].ToString();
                loAsiLin._Debe = Convert.ToDecimal(loRow["debe"]);
                loAsiLin._Haber = Convert.ToDecimal(loRow["haber"]);

                // nuevo WPM
                if (string.IsNullOrEmpty(loAsiLin._Cuenta.Trim()))
                {
                    this._Error_Message = this._Error_Message +
                                          "No se puede asignar la cuenta " + lcCuenta + " "+ loRow["idcfg"] + " ejercicio: " + loRow["ejercicio"] + " empresa: " + loRow["empresa"] + " asiento: " + loRow["numero"] + " ) \r\n" +
                                          "Mensaje asiento: " + loAsiento._Mensaje_Error.Trim() + "\r\n";
                }
                // nuevo WPM

                if (!String.IsNullOrEmpty(loRow["tipoiva"].ToString().Trim()) || !String.IsNullOrEmpty(loRow["tiporet"].ToString().Trim()))
                {
                    lcCodigo = loRow["codigo"].ToString().Trim();
                    if (!String.IsNullOrWhiteSpace(lcCodigo))
                    {
                        if (FUNCTIONS._Es_Cliente(lcCodigo))
                        {
                            if (toExcepcion._modoret || toExcepcion._iva_tipo)
                            {
                                lbEsCliente = true;
                                loCliente._Codigo = lcCodigo;
                            }
                        }
                        else if (FUNCTIONS._Es_Proveedor(lcCodigo))
                        {
                            loAsiento._Proveedor = lcCodigo;
                            if (toExcepcion._modoret || toExcepcion._iva_tipo)
                            {
                                lbEsProveedor = true;
                                loProveedor._Codigo = lcCodigo;
                            }
                        }
                    }

                    if (!String.IsNullOrEmpty(loRow["tipoiva"].ToString().Trim()))
                    {
                        // datos para cliente contado
                        if (Convert.ToBoolean(loRow["contado"]) && String.IsNullOrWhiteSpace(loAsiento._Cif))
                        {
                            loAsiento._Cif = loRow["cif"].ToString();
                            loAsiento._Nombre = loRow["nombre"].ToString();
                        }
                        loAsiLin._DatosIva._Cuenta = lcCodigo;
                        loAsiLin._DatosIva._Fecha = Convert.ToDateTime(loRow["fechafra"]);

                        if (loRow["fechaoper"] != null)
                            loAsiLin._DatosIva._FechaOper = Convert.ToDateTime(loRow["fechafra"]);
                        else
                            loAsiLin._DatosIva._FechaOper = Convert.ToDateTime(loRow["fechaoper"]);

                        loAsiLin._DatosIva._Bimpo = Convert.ToDecimal(loRow["basefactura"]);
                        loAsiLin._DatosIva._BimpoDiv = Convert.ToDecimal(loRow["basefactura"]);
                        loAsiLin._DatosIva._IVA = loAsiLin._DatosIva._IVADiv = Convert.ToDecimal(loRow["debe"]) + Convert.ToDecimal(loRow["haber"]);
                        loAsiLin._DatosIva._TipoIva = loRow["tipoiva"].ToString();
                        loAsiLin._DatosIva._PorcenRec = Convert.ToDecimal(loRow["porcenrec"]);
                        loAsiLin._DatosIva._Recargo = Convert.ToDecimal(loRow["importrec"]);
                        loAsiLin._DatosIva._Comunitari = Convert.ToBoolean(loRow["comunitario"]);

                        if (!toExcepcion._iva_tipo)
                            loAsiLin._DatosIva._Tipo = Convert.ToInt16(loRow["Tipo"]);
                        else
                        {
                            if (lbEsCliente)
                                loAsiLin._DatosIva._Tipo = (int)loCliente._TipoCliente;
                            else if (lbEsProveedor)
                                loAsiLin._DatosIva._Tipo = (int)loProveedor._TipoProveedor;
                        }

                        loAsiLin._DatosIva._Orden = Convert.ToInt32(loRow["orden"]);
                        loAsiLin._DatosIva._Liquidacion = Convert.ToInt16(loRow["liquidacion"]);
                        loAsiLin._DatosIva._LiqOp = Convert.ToInt16(loRow["liquidacion"]);
                    }

                    if (!String.IsNullOrEmpty(loRow["tiporet"].ToString().Trim()))
                    {
                        loAsiLin._DatosRetencion._Cuenta = lcCodigo;
                        loAsiLin._DatosRetencion._Fecha = Convert.ToDateTime(loRow["fechafra"]);
                        loAsiLin._DatosRetencion._Base = Convert.ToDecimal(loRow["basefactura"]);
                        loAsiLin._DatosRetencion._CodigoRetencion = loRow["tiporet"].ToString();
                        loAsiLin._DatosRetencion._PorcenRetencion = Convert.ToDecimal(loRow["porcenret"]);
                        loAsiLin._DatosRetencion._Retencion = Convert.ToDecimal(loRow["importret"]);

                        if (!toExcepcion._modoret)
                            loAsiLin._DatosRetencion._ModoRetencion = Convert.ToBoolean(loRow["modoret"]) ? 2 : 1;
                        else
                        {
                            if (lbEsCliente)
                                loAsiLin._DatosRetencion._ModoRetencion = loCliente._RetencionBaseFactura == Cliente.TipoRetencion.SobreBase ? 2 : 1;
                            else if (lbEsProveedor)
                                loAsiLin._DatosRetencion._ModoRetencion = loProveedor._RetencionBaseFactura == Proveedor.TipoRetencion.SobreBase ? 2 : 1;
                        }
                    }
                }

                // wpm.. ojo
                //loAsiento._AddLinea(loAsiLin);

            }

            if (!loAsiento._Save())
            {
                lcGuid = "";
                if (!string.IsNullOrEmpty(loAsiento._Mensaje_Error))
                {
                    /// WPM
                    DataRow loRow = toAsiento.Rows[0];
                    this._Error_Message = this._Error_Message +
                                          "No se puede generar el asiento " + loRow["idcfg"] + " ejercicio: " + loRow["ejercicio"] + " empresa: " + loRow["empresa"] + " asiento : " + loRow["numero"] + " )" +
                                          loAsiento._Mensaje_Error + "\r\n";
                }
            }
            else
            {
                lcGuid = loAsiento._Guid;

           
            }

            return lcGuid;
        }

        #endregion Asiento Contables sin documentos


        /// <summary>
        /// Método para crear cuenta contable, cliente o proveedor en Sage50
        /// previamente se han realizado las validaciones, de que la cuenta no exite, logintud del campo, etc.
        /// para este ejemplo, solo pasaremos 3 parametros
        /// </summary>
        /// <param name="tsCuenta"></param>
        /// <param name="tsNombre"></param>
        /// <param name="tsCIF"></param>
        public void CrearCuenta_ClienteProveedor(string tsCuenta, string tsNombre, string tsCIF = "")
        {
            if (string.IsNullOrEmpty(tsCuenta))
                return;

            bool llOk = false;
            string lsMensajeError = string.Empty;

            // validamos si es cliente
            if (sage.ew.functions.FUNCTIONS._Es_Cliente(tsCuenta))
            {
                Cliente _oCliente = new Cliente();
                _oCliente._Codigo = tsCuenta;
                if (!_oCliente._Existe_Registro())
                {
                    _oCliente._Nombre = tsNombre;
                    _oCliente._NIF = tsCIF;

                    llOk = _oCliente._Save();

                    if (!llOk || string.IsNullOrEmpty(_oCliente._Error_Message))
                        lsMensajeError = _oCliente._Error_Message.Trim();
                }
                _oCliente = null;
            }
            else
            {
                // validamos si es proveedor
                if (sage.ew.functions.FUNCTIONS._Es_Proveedor(tsCuenta))
                {
                    Proveedor _oProveedor = new Proveedor();
                    _oProveedor._Codigo = tsCuenta;
                    if (!_oProveedor._Existe_Registro())
                    {
                        _oProveedor._Nombre = tsNombre;
                        _oProveedor._NIF = tsCIF;

                        llOk = _oProveedor._Save();

                        if (!llOk || string.IsNullOrEmpty(_oProveedor._Error_Message))
                            lsMensajeError = _oProveedor._Error_Message.Trim();
                    }
                    _oProveedor = null;
                }
                else
                {
                    // Creamos una cuenta contable

                    Cuenta _oCuenta = new Cuenta();
                    _oCuenta._Codigo = tsCuenta;
                    if (!_oCuenta._Existe_Registro())
                    {
                        _oCuenta._Nombre = tsNombre;
                        _oCuenta._CIF = tsCIF;

                        llOk = _oCuenta._Save();

                        if (!llOk || string.IsNullOrEmpty(_oCuenta._Error_Message))
                            lsMensajeError = _oCuenta._Error_Message.Trim();
                    }
                    _oCuenta = null;

                }
            }


            if (string.IsNullOrEmpty(lsMensajeError))
                MessageBox.Show(lsMensajeError);


        }
    }

    public class Excepcion
    {
        public Boolean _modoret = false;       // IVAREPER y IVASOPOR origen sin modoret, se debe aplicar el modoret de la ficha del cliente o proveedor.
        public Boolean _iva_tipo = false;      // IVAREPER y IVASOPOR origen sin tipo, se debe aplicar el tipo de la ficha del cliente o proveedor.
    }

}

