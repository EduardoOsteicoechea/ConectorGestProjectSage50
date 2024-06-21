using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
//Sage50
using sage.ew.ewbase;
using sage.ew.cliente;
using sage.ew.articulo;
using sage.ew.docsven;
using sage.ew.docventatpv;
using sage.ew.db;
using sage.ew.lote.Clases;
// propio
using ExampleSatelite.Sage50.Datos;
using System.Windows.Forms;
using sage.ew.formul.UserControls;

namespace ExampleSatelite.Sage50.Negocio
{
    public class SalesDocument
    {


    }


    #region Presupuestos de venta
    public class PresupuestoVenta : BaseDocument
    {
        ewDocVentaPRESUP _oDocVentaPRE;
        ewDocVentaLinPRESUP _oLinia;

        public Boolean _Create(clsPresuven toPresuven)
        {
            this._Error_Message = string.Empty;
            bool llOk = false, llContinue = false;

            if (toPresuven.Cabecera != null && toPresuven.Lineas != null && toPresuven.Lineas.Count != 0)
            {
                Cliente loCliente;
                FuturoCliente loPotencial;

                bool llExiteCliente = false;

                _oDocVentaPRE = new ewDocVentaPRESUP();
                _oLinia = new ewDocVentaLinPRESUP();

                toPresuven.Cabecera.empresa = this._Empresa;
                toPresuven.Cabecera.ejercicio = this._Ejercicio;
                toPresuven.Cabecera.letra = toPresuven.Cabecera.letra.Trim().PadLeft(2, ' ');
                toPresuven.Cabecera.numero = toPresuven.Cabecera.numero.Trim().PadLeft(10, ' ');

                // Validamos que existe código de cliente
                if (toPresuven.Direccion != null)
                {
                    // Usamos el CIF, para ubicar el primer registro que coincida en la tabla de cliente
                    if (string.IsNullOrEmpty(toPresuven.Cabecera.cliente) && !string.IsNullOrEmpty(toPresuven.Direccion.cif))
                        toPresuven.Cabecera.cliente = ReplicatedDB.SQLValor("CLIENTES", "CIF", toPresuven.Direccion.cif, "CODIGO").ToString();
                }

                // Verificamos si es un futuro cliente (Potencial)
                // Aplicamos el codigo de clientes varios si es necesario
                if (toPresuven.Cabecera.futuro)
                {
                    loCliente = null;
                    loPotencial = new FuturoCliente();
                    if (string.IsNullOrEmpty(toPresuven.Cabecera.cliente.Trim()))
                    {
                        loPotencial._Obtener_Numero();
                        loPotencial._Nombre = toPresuven.Direccion.nombre;
                        loPotencial._CIF = toPresuven.Direccion.cif;
                        loPotencial._Direccion = toPresuven.Direccion.direccion;
                        loPotencial._CodPost = toPresuven.Direccion.codpos;
                        loPotencial._Poblacion = toPresuven.Direccion.poblacion;
                        loPotencial._Provincia = toPresuven.Direccion.provincia;
                        loPotencial._Pais = toPresuven.Direccion.pais;
                        loPotencial._Email = toPresuven.Direccion.email;
                        loPotencial._Telefono = toPresuven.Direccion.telefono;
                        loPotencial._Contacto = toPresuven.Direccion.contacto;
                        loPotencial._TarifaVenta = toPresuven.Cabecera.tarifa;

                        loPotencial._Save();
                        toPresuven.Cabecera.cliente = loPotencial._Codigo;
                    }
                    else
                    {
                        loPotencial._Codigo = toPresuven.Cabecera.cliente.Trim();
                    }
                    llExiteCliente = loPotencial._Existe_Registro();
                }
                else
                {
                    loPotencial = null;
                    toPresuven.Cabecera.cliente = (string.IsNullOrEmpty(toPresuven.Cabecera.cliente)) ? this._CliVarios : toPresuven.Cabecera.cliente;

                    // Abrimos el objeto de cliente
                    loCliente = new Cliente();
                    loCliente._Codigo = toPresuven.Cabecera.cliente;

                    llExiteCliente = loCliente._Existe_Registro();
                }
                

                // comprobamos que exista el cliente para poder crear el pedido
                if (llExiteCliente)
                {
                    // comprobamos si el pedido ya existe
                    if (_oDocVentaPRE._Existe(toPresuven.Cabecera.empresa, toPresuven.Cabecera.numero, toPresuven.Cabecera.letra))
                    {
                        // si ya existe, lo cargamos
                        _oDocVentaPRE._Load(toPresuven.Cabecera.empresa, toPresuven.Cabecera.numero, toPresuven.Cabecera.letra);
                        if (_oDocVentaPRE._Cabecera._Cliente != toPresuven.Cabecera.cliente.Trim())
                            this._Error_Message += "El Código del cliente del presupuesto es " + _oDocVentaPRE._Cabecera._Cliente + ", y se esta informando un cliente es diferente " + toPresuven.Cabecera.cliente + "\r\n";
                        else
                            llContinue = true;
                    }
                    else
                    {
                        // no existe, lo creamos
                        _oDocVentaPRE._New(toPresuven.Cabecera.empresa, toPresuven.Cabecera.letra, toPresuven.Cabecera.numero);
                        _oDocVentaPRE._Cabecera._Cliente = toPresuven.Cabecera.cliente;
                        _oDocVentaPRE._Cabecera._Futuro = toPresuven.Cabecera.futuro;
                        _oDocVentaPRE._Cabecera._Futuro_Cliente = toPresuven.Cabecera.futuro ? toPresuven.Cabecera.cliente : "";

                        llContinue = true;
                    }

                    //_oDocVentaPRE._Cabecera._Futuro
                    //_oDocVentaPRE._Cabecera._Futuro_Cliente
                    //_oDocVentaPRE._Cabecera._oFuturo_Cliente


                    if (llContinue)
                    {
                        // validamos la forma de pago
                        string lsFPago = ReplicatedDB.SQLValor("FPAG", "CODIGO", toPresuven.Cabecera.formapago, "CODIGO").ToString();
                        if (!string.IsNullOrEmpty(lsFPago))
                            _oDocVentaPRE._Cabecera._FormaPago = lsFPago;

    
                        if (!string.IsNullOrEmpty(toPresuven.Cabecera.comentario))
                            _oDocVentaPRE._Cabecera._Nota = toPresuven.Cabecera.comentario;

                        if (!string.IsNullOrEmpty(toPresuven.Cabecera.observaciones))
                            _oDocVentaPRE._Cabecera._Observacio = toPresuven.Cabecera.observaciones;

                        // Dirección de envío para el pedido de venta
                        if (toPresuven.Direccion != null)
                        {
                            if (loCliente._ClienteContado)
                            {
                                _oDocVentaPRE._Cabecera._DatosContado._Nombre = toPresuven.Direccion.nombre;
                                _oDocVentaPRE._Cabecera._DatosContado._Cif = toPresuven.Direccion.cif;
                                _oDocVentaPRE._Cabecera._DatosContado._Direccion = toPresuven.Direccion.direccion;
                                _oDocVentaPRE._Cabecera._DatosContado._CodPost = toPresuven.Direccion.codpos;
                                _oDocVentaPRE._Cabecera._DatosContado._Poblacion = toPresuven.Direccion.poblacion;
                                _oDocVentaPRE._Cabecera._DatosContado._Provincia = toPresuven.Direccion.provincia;
                                _oDocVentaPRE._Cabecera._DatosContado._Pais = toPresuven.Direccion.pais;
                                _oDocVentaPRE._Cabecera._DatosContado._Email = toPresuven.Direccion.email;
                                _oDocVentaPRE._Cabecera._DatosContado._Telefono = toPresuven.Direccion.telefono;
                                _oDocVentaPRE._Cabecera._DatosContado._Save();
                            }
                            else
                            {
                                // Obtenemos un datatable con las direcciones de la ficha del cliente
                                DataTable loDirecciones = loCliente._Direcciones();

                                // Buscamos la dirección dentro del datatable
                                DataRow[] loRow = loDirecciones.Select(String.Format("direccion = '{0}' AND codpos = '{1}' AND poblacion = '{2}' AND provincia = '{3}'", toPresuven.Direccion.direccion, toPresuven.Direccion.codpos, toPresuven.Direccion.poblacion, toPresuven.Direccion.provincia));

                                if (loRow.Length > 0)
                                {
                                    // Si la dirección existe, le aplicamos el número de la linea relacionado al pedido de venta
                                    _oDocVentaPRE._Cabecera._Env_cli = Convert.ToInt16(loRow[0]["linea"]);
                                }
                                else
                                {
                                    // damos de alta la nueva dirección de envío en la ficha del cliente
                                    Cliente.Direcciones.Direccion loDireccion = loCliente._TRelDirecciones._NewItem();
                                    loDireccion._Nombre = toPresuven.Direccion.nombre;
                                    loDireccion._Direccion = toPresuven.Direccion.direccion;
                                    loDireccion._CodPos = toPresuven.Direccion.codpos;
                                    loDireccion._Poblacion = toPresuven.Direccion.poblacion;
                                    loDireccion._Provincia = toPresuven.Direccion.provincia;
                                    loDireccion._Pais = toPresuven.Direccion.pais;
                                    loDireccion._Telefono = toPresuven.Direccion.telefono;
                                    loDireccion._Tipo = (int)Cliente.Direcciones.TiposDirecciones.Envios;   // indicamos que la dirección es de envios.
                                    // grabamos el registro
                                    loCliente._TRelDirecciones._SaveItem(loDireccion);
                                    //

                                    _oDocVentaPRE._Cabecera._Env_cli = loDireccion._Linia;
                                }

                                loDirecciones.Dispose();

                            }
                        }

                        if (toPresuven.Lineas != null && toPresuven.Lineas.Count != 0)
                        {
                            string lsCodigo = string.Empty;
                            
                            foreach (var LineaPedido in toPresuven.Lineas)
                            {
                                _oLinia = _oDocVentaPRE._AddLinea();


                                lsCodigo = ReplicatedDB.SQLValor("ARTICULO", "CODIGO", LineaPedido.articulo, "CODIGO").ToString();
                                if (!string.IsNullOrEmpty(lsCodigo))
                                {
                                    _oLinia._Articulo = lsCodigo;
                                    _oLinia._Talla = LineaPedido.talla;
                                    _oLinia._Color = LineaPedido.color;
                                    //_oLinia._Peso = LineaPedido.peso;
                                    //_oLinia._Cajas = LineaPedido.cajas;
                                    _oLinia._Unidades = LineaPedido.unidades;

                                    // si indicamos que aplique los precios que lee desde el objeto
                                    // de lo contrario, aplicará los precios calculados por Sage50
                                    toPresuven.Cabecera.precios = true;

                                    if (toPresuven.Cabecera.precios)
                                    {
                                        _oLinia._Precio = LineaPedido.precio;
                                        _oLinia._Dto1 = LineaPedido.dto1;
                                        _oLinia._Dto2 = LineaPedido.dto2;
                                        _oLinia._Recalcular_Importe();
                                    }

                                }
                                else
                                {
                                    // Es una linea de comentario
                                    if (!string.IsNullOrEmpty(LineaPedido.definicion))
                                    {
                                        _oLinia._Definicion = LineaPedido.definicion;
                                        _oLinia._TipoIva = "";
                                    }
                                    else
                                    {
                                        this._Error_Message += "El código de articulo " + LineaPedido.articulo + ", no existe\r\n";
                                    }


                                }
                                _oLinia._Save();
                                //                            _oDocVentaPED._AddLinea(_oLinia);
                            }

                        }

                        // grabamos el pedido
                        _oDocVentaPRE._Totalizar();
                        llOk = _oDocVentaPRE._Save();

                        if (llOk)
                        {
                            toPresuven.Cabecera.numero = _oDocVentaPRE._Numero;
                        }
                        else
                        {
                            string lsNumero = string.IsNullOrEmpty(toPresuven.Cabecera.letra) ? "" : toPresuven.Cabecera.letra;
                            lsNumero += toPresuven.Cabecera.numero;
                            this._Error_Message += "No se a podido guardar el presupuesto de venta :" + lsNumero + "\r\n";
                        }
                    }
                }
                else
                {
                    this._Error_Message += "El Código de cliente " + toPresuven.Cabecera.cliente + ", no existe\r\n";
                }
            }
            else
            {
                if (toPresuven.Cabecera == null)
                    this._Error_Message += "Los datos de la cabecera presupuesto son obligatorios\r\n";
                else
                    this._Error_Message += "Es obligatorio insertar un articulo para poder generar el presupuesto\r\n";
            }

            return llOk;
        }
    }

    #endregion Presupuestos de venta

    #region Pedidos de venta
    public class PedidoVenta : BaseDocument
    {
        ewDocVentaPED _oDocVentaPED;
        ewDocVentaTPV _oDocVenta;
        ewDocVentaLinPED _oLinia;

        public Boolean _Create(clsPediven toPediven)
        {
            this._Error_Message = string.Empty;
            bool llOk = false, llContinue = false;
            
            if (toPediven.Cabecera != null && toPediven.Lineas!= null && toPediven.Lineas.Count != 0)
            {
                Cliente loCliente;

                _oDocVentaPED = new ewDocVentaPED();
                //_oDocVenta = new ewDocVentaTPV();
                _oLinia = new ewDocVentaLinPED();

                toPediven.Cabecera.empresa = this._Empresa;
                toPediven.Cabecera.ejercicio = this._Ejercicio;
                toPediven.Cabecera.letra = toPediven.Cabecera.letra.Trim().PadLeft(2, ' ');
                toPediven.Cabecera.numero = toPediven.Cabecera.numero.Trim().PadLeft(10, ' ');

                // Validamos que existe código de cliente
                if (toPediven.Direccion != null)
                {
                    // Usamos el CIF, para ubicar el primer registro que coincida en la tabla de cliente
                    if (string.IsNullOrEmpty(toPediven.Cabecera.cliente) && !string.IsNullOrEmpty(toPediven.Direccion.cif))
                        toPediven.Cabecera.cliente = ReplicatedDB.SQLValor("CLIENTES", "CIF", toPediven.Direccion.cif, "CODIGO").ToString();
                }

                // Aplicamos el codigo de clientes varios si es necesario
                toPediven.Cabecera.cliente = (string.IsNullOrEmpty(toPediven.Cabecera.cliente)) ? this._CliVarios : toPediven.Cabecera.cliente;

                // Abrimos el objeto de cliente
                loCliente = new Cliente();
                loCliente._Codigo = toPediven.Cabecera.cliente;

                // comprobamos que exista el cliente para poder crear el pedido
                if (loCliente._Existe_Registro())
                {
                    // comprobamos si el pedido ya existe
                    if (_oDocVentaPED._Existe(toPediven.Cabecera.empresa, toPediven.Cabecera.numero, toPediven.Cabecera.letra))
                    {
                        // si ya existe, lo cargamos
                        _oDocVentaPED._Load(toPediven.Cabecera.empresa, toPediven.Cabecera.numero, toPediven.Cabecera.letra);
                        if (_oDocVentaPED._Cabecera._Cliente != toPediven.Cabecera.cliente)
                            this._Error_Message += "El Código del cliente del pedido es " + _oDocVentaPED._Cabecera._Cliente + ", y se esta informando un cliente es diferente " + toPediven.Cabecera.cliente + "\r\n";
                        else
                            llContinue = true;
                    }
                    else
                    {
                        // no existe, lo creamos
                        _oDocVentaPED._New(toPediven.Cabecera.empresa, toPediven.Cabecera.letra, toPediven.Cabecera.numero);
                        _oDocVentaPED._Cabecera._Cliente = toPediven.Cabecera.cliente;
                        llContinue = true;
                    }

                    if (llContinue)
                    {
                        // validamos la forma de pago
                        string lsFPago = ReplicatedDB.SQLValor("FPAG", "CODIGO", toPediven.Cabecera.formapago, "CODIGO").ToString();
                        if (!string.IsNullOrEmpty(lsFPago))
                            _oDocVentaPED._Cabecera._FormaPago = lsFPago;

                        if (!string.IsNullOrEmpty(toPediven.Cabecera.refercli))
                            _oDocVentaPED._Cabecera._Refercli = toPediven.Cabecera.refercli;

                        if (!string.IsNullOrEmpty(toPediven.Cabecera.comentario))
                            _oDocVentaPED._Cabecera._Nota = toPediven.Cabecera.comentario;

                        if (!string.IsNullOrEmpty(toPediven.Cabecera.observaciones))
                            _oDocVentaPED._Cabecera._Observacio = toPediven.Cabecera.observaciones;

                        // Dirección de envío para el pedido de venta
                        if (toPediven.Direccion != null)
                        {
                            if (loCliente._ClienteContado)
                            {
                                _oDocVentaPED._Cabecera._DatosContado._Nombre = toPediven.Direccion.nombre;
                                _oDocVentaPED._Cabecera._DatosContado._Cif = toPediven.Direccion.cif;
                                _oDocVentaPED._Cabecera._DatosContado._Direccion = toPediven.Direccion.direccion;
                                _oDocVentaPED._Cabecera._DatosContado._CodPost = toPediven.Direccion.codpos;
                                _oDocVentaPED._Cabecera._DatosContado._Poblacion = toPediven.Direccion.poblacion;
                                _oDocVentaPED._Cabecera._DatosContado._Provincia = toPediven.Direccion.provincia;
                                _oDocVentaPED._Cabecera._DatosContado._Pais = toPediven.Direccion.pais;
                                _oDocVentaPED._Cabecera._DatosContado._Email = toPediven.Direccion.email;
                                _oDocVentaPED._Cabecera._DatosContado._Telefono = toPediven.Direccion.telefono;
                                _oDocVentaPED._Cabecera._DatosContado._Save();
                            }
                            else
                            {
                                // Obtenemos un datatable con las direcciones de la ficha del cliente
                                DataTable loDirecciones = loCliente._Direcciones();

                                // Buscamos la dirección dentro del datatable
                                DataRow[] loRow = loDirecciones.Select(String.Format("direccion = '{0}' AND codpos = '{1}' AND poblacion = '{2}' AND provincia = '{3}'", toPediven.Direccion.direccion, toPediven.Direccion.codpos, toPediven.Direccion.poblacion, toPediven.Direccion.provincia));

                                if (loRow.Length > 0)
                                {
                                    // Si la dirección existe, le aplicamos el número de la linea relacionado al pedido de venta
                                    _oDocVentaPED._Cabecera._Env_cli = Convert.ToInt16(loRow[0]["linea"]);
                                }
                                else
                                {
                                    // damos de alta la nueva dirección de envío en la ficha del cliente
                                    Cliente.Direcciones.Direccion loDireccion = loCliente._TRelDirecciones._NewItem();
                                    loDireccion._Nombre = toPediven.Direccion.nombre;
                                    loDireccion._Direccion = toPediven.Direccion.direccion;
                                    loDireccion._CodPos = toPediven.Direccion.codpos;
                                    loDireccion._Poblacion = toPediven.Direccion.poblacion;
                                    loDireccion._Provincia = toPediven.Direccion.provincia;
                                    loDireccion._Pais = toPediven.Direccion.pais;
                                    loDireccion._Telefono = toPediven.Direccion.telefono;
                                    loDireccion._Tipo = (int)Cliente.Direcciones.TiposDirecciones.Envios;   // indicamos que la dirección es de envios.
                                    // grabamos el registro
                                    loCliente._TRelDirecciones._SaveItem(loDireccion);
                                    //

                                    _oDocVentaPED._Cabecera._Env_cli = loDireccion._Linia;
                                }

                                loDirecciones.Dispose();

                            }
                        }

                        if (toPediven.Lineas != null && toPediven.Lineas.Count != 0)
                        {
                            string lsCodigo = string.Empty;
                            //_oLinia
                            //ewDocVentaLinPED loLinia = new ewDocVentaLinPED();
                            foreach (var LineaPedido in toPediven.Lineas)
                            {
                                _oLinia = _oDocVentaPED._AddLinea();
                                //_oLinia = new ewDocVentaLinPED();


                                lsCodigo = ReplicatedDB.SQLValor("ARTICULO", "CODIGO", LineaPedido.articulo, "CODIGO").ToString();
                                if (!string.IsNullOrEmpty(lsCodigo))
                                {
                                    _oLinia._Articulo = lsCodigo;
                                    _oLinia._Talla = LineaPedido.talla;
                                    _oLinia._Color = LineaPedido.color;
                                    //_oLinia._Peso = LineaPedido.peso;
                                    //_oLinia._Cajas = LineaPedido.cajas;
                                    _oLinia._Unidades = LineaPedido.unidades;

                                    // si indicamos que aplique los precios que lee desde el objeto
                                    // de lo contrario, aplicará los precios calculados por Sage50
                                    toPediven.Cabecera.precios = true;

                                    if (toPediven.Cabecera.precios)
                                    {
                                        _oLinia._Precio = LineaPedido.precio;
                                        _oLinia._Dto1 = LineaPedido.dto1;
                                        _oLinia._Dto2 = LineaPedido.dto2;
                                        _oLinia._Recalcular_Importe();
                                    }

                                }
                                else
                                {
                                    // Es una linea de comentario
                                    if (!string.IsNullOrEmpty(LineaPedido.definicion))
                                    {
                                        _oLinia._Definicion = LineaPedido.definicion;
                                        _oLinia._TipoIva = "";
                                    }
                                    else
                                    {
                                        this._Error_Message += "El código de articulo " + LineaPedido.articulo + ", no existe\r\n";
                                    }

                                        
                                }
                                _oLinia._Save();
    //                            _oDocVentaPED._AddLinea(_oLinia);
                            }

                        }

                        // grabamos el pedido
                        _oDocVentaPED._Totalizar();
                        llOk = _oDocVentaPED._Save();

                        if (llOk)
                        {
                            toPediven.Cabecera.numero = _oDocVentaPED._Numero;
                        }
                        else
                        {
                            string lsNumero = string.IsNullOrEmpty(toPediven.Cabecera.letra) ? "" : toPediven.Cabecera.letra;
                            lsNumero += toPediven.Cabecera.numero;
                            this._Error_Message += "No se a podido guardar el pedido de venta :"+ lsNumero + "\r\n";
                        }
                    }
                }
                else
                {
                    this._Error_Message += "El Código de cliente " + toPediven.Cabecera.cliente + ", no existe\r\n";
                }
            }
            else
            {
                if (toPediven.Cabecera == null)
                    this._Error_Message += "Los datos de la cabecera pedido son obligatorios\r\n";
                else
                    this._Error_Message += "Es obligatorio insertar un articulo para poder generar el pedido\r\n";
            }

            return llOk;
        }

        public Boolean _Update(clsPediven toPediven)
        {
            this._Error_Message = string.Empty;
            bool llOk = false; //llContinue = false;

            return llOk;
        }

        public Boolean _Delete(clsPediven toPediven)
        {
            this._Error_Message = string.Empty;
            bool llOk = false;

            if (toPediven.Cabecera != null)
            {
                _oDocVentaPED = new ewDocVentaPED();
                if (_oDocVentaPED._Existe(toPediven.Cabecera.empresa, toPediven.Cabecera.numero, toPediven.Cabecera.letra))
                {
                    // si ya existe, lo cargamos
                    _oDocVentaPED._Load(toPediven.Cabecera.empresa, toPediven.Cabecera.numero, toPediven.Cabecera.letra);
                    _oDocVentaPED._Delete();
                    _oDocVentaPED._Save();

                    if (!string.IsNullOrEmpty(_oDocVentaPED._Mensaje_Error))
                        this._Error_Message += _oDocVentaPED._Mensaje_Error + "\r\n";
                    else
                        llOk = true;

                }
            }
            else
            {
                this._Error_Message += "Los datos de la cabecera pedido son obligatorios\r\n";
            }

            return llOk;
        }


        public Boolean _Delete_Lineas()
        {
            this._Error_Message = string.Empty;
            bool llOk = false;

            return llOk;
        }

        public Boolean _to_AlbaranVenta(clsPediven toPediven, string tsAlbaranNumero, string tsAlbaranLetra, DateTime? tdAlbaraFecha)
        {
            this._Error_Message = string.Empty;
            bool llOk = false, llTraspasar = true;

            if (toPediven.Cabecera != null)
            {
                _oDocVentaPED = new ewDocVentaPED();
                if (_oDocVentaPED._Existe(toPediven.Cabecera.empresa, toPediven.Cabecera.numero, toPediven.Cabecera.letra))
                {
                    _oDocVentaPED._Load(toPediven.Cabecera.empresa, toPediven.Cabecera.numero, toPediven.Cabecera.letra);


                    DateTime ldAlbaranFecha = DateTime.Now;
                    if (tdAlbaraFecha != null)
                        ldAlbaranFecha = (DateTime)tdAlbaraFecha;

                    List<clsPedivenLineas> LineasTraspaso = new List<clsPedivenLineas>();

                    // recorremos las lineas del pedido de venta
                    foreach (ewDocVentaLinPED loLinea in _oDocVentaPED._Lineas)
                    {
                        if (this._FlagStock)
                        {
                            // obtenemos el stock actual del articulo, para el almacen y la fecha indicada
                            loLinea._oArticulo._Stock._Almacen = this._Almacen;
                            loLinea._oArticulo._Stock._Fecha = ldAlbaranFecha;
                            loLinea._oArticulo._Stock._GetStock();

                            // Si no hay stock
                            if ((loLinea._oArticulo._Stock._Total_Existencias - loLinea._Unidades) < 0)
                            {
                                this._Error_Message += "No se puede traspasar el pedido.\r\n" +
                                                        "No hay stock disponible para el artículo" + loLinea._Articulo + ".\r\n";
                                llTraspasar = false;
                                break;
                            }
                        }

                        if (toPediven.Lineas.Any())
                        {
                            clsPedivenLineas loLinea2 = toPediven.Lineas.Find(x => x.linea == loLinea._Linea);
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
                        // creamos el objeto para el albarán de venta
                        _oDocVenta = new ewDocVentaTPV();

                        // Diccionario de parametros que se envian para la generación del traspaso a la clase del CORE del Sage50
                        Dictionary<string, object> loDicParam = new Dictionary<string, object>();
                        loDicParam.Add("tipodoc", 2);           // 2 Albarán de venta
                        loDicParam.Add("fecha", ldAlbaranFecha);
                        loDicParam.Add("almacen", this._Almacen);
                        loDicParam.Add("divisa", _oDocVentaPED._Divisa);
                        loDicParam.Add("pedidotoalbaran", true);
                        loDicParam.Add("traspasoportes", true);
                        loDicParam.Add("portes", _oDocVentaPED._Pie._Portes);

                        if (!string.IsNullOrWhiteSpace(tsAlbaranNumero) && _oDocVenta._Existe(toPediven.Cabecera.empresa, tsAlbaranNumero, tsAlbaranLetra))
                        {
                            _oDocVenta._Load(toPediven.Cabecera.empresa, tsAlbaranNumero, tsAlbaranLetra);
                            if (!_oDocVenta._Solo_Lectura)
                            {
                                if (_oDocVenta._Cabecera._Cliente == _oDocVentaPED._Cabecera._Cliente)
                                    loDicParam.Add("docudestino", _oDocVenta);
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
                            string lcNumero = _oDocVentaPED._To_Albaran(loDicParam);

                            if (_oDocVentaPED._Documento_Traspasado_Totalmente())
                                _oDocVentaPED._Cabecera._Traspasado = true;

                            _oDocVentaPED._Desbloquear_Documento();

                            if (string.IsNullOrEmpty(_oDocVentaPED._Mensaje_Error))
                            {
                                llOk = string.IsNullOrEmpty(lcNumero.Trim()) ? true : false;
                            }
                            else
                            {
                                this._Error_Message += _oDocVentaPED._Mensaje_Error + ".\r\n";
                            }
                        }
                    }
                }
                else
                {
                    this._Error_Message += "El pedido de venta no existe.\r\n";
                }
            }
            else
            {
                this._Error_Message += "Los datos de la cabecera pedido son obligatorios.\r\n";
            }

            return llOk;
        }

        public Boolean _Exist(string tsNumero, string tsLetra)
        {
            if (_oDocVentaPED == null)
                _oDocVentaPED = new ewDocVentaPED();

            bool llOk = _oDocVentaPED._Existe(this._Empresa, tsNumero, tsLetra);
            return llOk;
        }

        public clsPediven _LoadEntity(string tsNumero, string tsLetra)
        {
            clsPediven loPediven = new clsPediven();

            _oDocVentaPED = new ewDocVentaPED();
            _oLinia = new ewDocVentaLinPED();

            _oDocVentaPED._Load(this._Empresa, tsNumero, tsLetra);

            // Pasamos los valores a la cabecera de la entidad
            loPediven.Cabecera.empresa = this._Empresa;
            loPediven.Cabecera.ejercicio = this._Ejercicio;
            loPediven.Cabecera.letra = _oDocVentaPED._Letra;
            loPediven.Cabecera.fecha = _oDocVentaPED._Fecha;
            loPediven.Cabecera.numero = _oDocVentaPED._Numero;
            loPediven.Cabecera.cliente  = _oDocVentaPED._Cabecera._Cliente;
            loPediven.Cabecera.almacen = _oDocVentaPED._Cabecera._Almacen;
            loPediven.Cabecera.formapago = _oDocVentaPED._Cabecera._FormaPago;
            loPediven.Cabecera.comentario  = _oDocVentaPED._Cabecera._Nota;
            loPediven.Cabecera.observaciones  = _oDocVentaPED._Cabecera._Observacio;
            loPediven.Cabecera.refercli  = _oDocVentaPED._Cabecera._Refercli;
            loPediven.Cabecera.precios = false;
            loPediven.Cabecera.enc_cli = _oDocVentaPED._Cabecera._Env_cli;
            loPediven.Cabecera.traspasado = _oDocVentaPED._Cabecera._Traspasado;
            loPediven.Cabecera.cancelado = _oDocVentaPED._Cabecera._Cancelado;


            string lsNombreCliente = ReplicatedDB.SQLValor("CLIENTES", "CODIGO", loPediven.Cabecera.cliente, "NOMBRE").ToString();
            if (!string.IsNullOrEmpty(lsNombreCliente))
                loPediven.Cabecera.nombre = lsNombreCliente;


            foreach (ewDocVentaLinPED _oLinia in _oDocVentaPED._Lineas)
            {
                clsPedivenLineas LineaPedido = new clsPedivenLineas();
                LineaPedido.linea = _oLinia._Linea;
                LineaPedido.articulo = _oLinia._Articulo;
                LineaPedido.definicion = _oLinia._Definicion;
                LineaPedido.talla = _oLinia._Talla;
                LineaPedido.color = _oLinia._Color;
                LineaPedido.unidades = _oLinia._Unidades;
                LineaPedido.cajas = _oLinia._Cajas;
                LineaPedido.peso = _oLinia._Peso;
                LineaPedido.precio = _oLinia._Precio;
                LineaPedido.dto1 = _oLinia._Dto1;
                LineaPedido.dto2 = _oLinia._Dto2;
                LineaPedido.tipoiva = _oLinia._TipoIva;

                loPediven.Lineas.Add(LineaPedido);
            }

            // dirección de envío
            Cliente loCliente = new Cliente();
            loCliente._Codigo = loPediven.Cabecera.cliente;

            if (loCliente._ClienteContado)
            {
                loPediven.Direccion.nombre = _oDocVentaPED._Cabecera._DatosContado._Nombre;
                loPediven.Direccion.cif = _oDocVentaPED._Cabecera._DatosContado._Cif;
                loPediven.Direccion.direccion = _oDocVentaPED._Cabecera._DatosContado._Direccion;
                loPediven.Direccion.codpos = _oDocVentaPED._Cabecera._DatosContado._CodPost;
                loPediven.Direccion.poblacion = _oDocVentaPED._Cabecera._DatosContado._Poblacion;
                loPediven.Direccion.provincia = _oDocVentaPED._Cabecera._DatosContado._Provincia;
                loPediven.Direccion.pais = _oDocVentaPED._Cabecera._DatosContado._Pais;
                loPediven.Direccion.email = _oDocVentaPED._Cabecera._DatosContado._Email;
                loPediven.Direccion.telefono = _oDocVentaPED._Cabecera._DatosContado._Telefono;
            }
            else
            {
                Cliente.Direcciones.Direccion loDireccion = loCliente._TRelDirecciones._GetItemByLinea(loPediven.Cabecera.enc_cli);
                loPediven.Direccion.nombre = loDireccion._Nombre;
                loPediven.Direccion.cif = loCliente._NIF;
                loPediven.Direccion.direccion = loDireccion._Direccion;
                loPediven.Direccion.codpos = loDireccion._CodPos;
                loPediven.Direccion.poblacion = loDireccion._Poblacion;
                loPediven.Direccion.provincia = loDireccion._Provincia;
                loPediven.Direccion.pais = loDireccion._Pais;
                loPediven.Direccion.telefono = loDireccion._Telefono;
                //loPediven.Direccion.email = "";

                loDireccion = null;
            }
            loCliente = null;

            return loPediven;
        }


        public override int _ObtenerPrecio(Cliente toCliente, Articulo toArticulo, DateTime tdFecha, ref DataTable tdtResposta, ref DataTable tdtRegalo, string tcDivisa, decimal tnUnidades = 0.0M, string tcObra = "", string tcTalla = "", string tcColor = "", string tcTarifaAlbaran = "", bool tlEsCaja = false, decimal tnCajasReales = 0.0M, DataTable tdtCurTc = null, string tcCodigoAgrupacion = "")
        {
            return base._ObtenerPrecio(toCliente, toArticulo, tdFecha, ref tdtResposta, ref tdtRegalo, tcDivisa, tnUnidades, tcObra, tcTalla, tcColor, tcTarifaAlbaran, tlEsCaja, tnCajasReales, tdtCurTc, tcCodigoAgrupacion);
        }


    }
    #endregion Pedidos de venta
    
    #region Albarán de venta
    public class AlbaranVenta : BaseDocument
    {
        ewDocVentaTPV _oDocVenta;
        ewDocVentaLinTPV _oLinia;
        LinVenDet<LinVenDetLotes> _oLinVenDetLotes;

        public Boolean _Create(clsAlbaven toAlbaven)
        {
            this._Error_Message = string.Empty;
            bool llOk = false, llContinue = false;

            if (toAlbaven.Cabecera != null && toAlbaven.Lineas != null && toAlbaven.Lineas.Count != 0)
            {
                Cliente loCliente;

                MessageBox.Show("INSIDE\n\nAlbaranVenta._Create");
                // HERE IS THE ISSUE
                // HERE IS THE ISSUE
                // HERE IS THE ISSUE
                // HERE IS THE ISSUE
                // HERE IS THE ISSUE
                // HERE IS THE ISSUE
                // HERE IS THE ISSUE
                // HERE IS THE ISSUE
                // HERE IS THE ISSUE

                //_oDocVenta = new ewDocVentaTPV();
                //_oLinia = new ewDocVentaLinTPV();

                toAlbaven.Cabecera.empresa = this._Empresa;
                toAlbaven.Cabecera.ejercicio = this._Ejercicio;
                toAlbaven.Cabecera.letra = toAlbaven.Cabecera.letra.Trim().PadLeft(2, ' ');
                toAlbaven.Cabecera.numero = toAlbaven.Cabecera.numero.Trim().PadLeft(10, ' ');

                // Validamos que existe código de cliente
                if (toAlbaven.Direccion != null)
                {
                    // Usamos el CIF, para ubicar el primer registro que coincida en la tabla de cliente
                    if (string.IsNullOrEmpty(toAlbaven.Cabecera.cliente) && !string.IsNullOrEmpty(toAlbaven.Direccion.cif))
                        toAlbaven.Cabecera.cliente = ReplicatedDB.SQLValor("CLIENTES", "CIF", toAlbaven.Direccion.cif, "CODIGO").ToString();
                }

                // Aplicamos el codigo de clientes varios si es necesario
                toAlbaven.Cabecera.cliente = (string.IsNullOrEmpty(toAlbaven.Cabecera.cliente)) ? this._CliVarios : toAlbaven.Cabecera.cliente;

                // Abrimos el objeto de cliente
                loCliente = new Cliente();
                loCliente._Codigo = toAlbaven.Cabecera.cliente;

                // comprobamos que exista el cliente para poder crear el pedido
                if (loCliente._Existe_Registro())
                {
                    //// comprobamos si el pedido ya existe
                    //if (_oDocVenta._Existe(toAlbaven.Cabecera.empresa, toAlbaven.Cabecera.numero, toAlbaven.Cabecera.letra))
                    //{
                    //    // si ya existe, lo cargamos
                    //    _oDocVenta._Load(toAlbaven.Cabecera.empresa, toAlbaven.Cabecera.numero, toAlbaven.Cabecera.letra);
                    //    if (_oDocVenta._Cabecera._Cliente != toAlbaven.Cabecera.cliente)
                    //        this._Error_Message += "El Código del cliente del pedido es " + _oDocVenta._Cabecera._Cliente + ", y se esta informando un cliente es diferente " + toAlbaven.Cabecera.cliente + "\r\n";
                    //    else
                    //        llContinue = true;
                    //}
                    //else
                    //{
                    //    // no existe, lo creamos
                    //    _oDocVenta._New(toAlbaven.Cabecera.empresa, toAlbaven.Cabecera.letra, toAlbaven.Cabecera.numero);
                    //    _oDocVenta._Cabecera._Cliente = toAlbaven.Cabecera.cliente;
                    //    llContinue = true;
                    //}

                    //if (llContinue)
                    //{
                    //    // validamos la forma de pago
                    //    string lsFPago = DB.SQLValor("FPAG", "CODIGO", toAlbaven.Cabecera.formapago, "CODIGO").ToString();
                    //    if (!string.IsNullOrEmpty(lsFPago))
                    //        _oDocVenta._Cabecera._FormaPago = lsFPago;

                    //    if (!string.IsNullOrEmpty(toAlbaven.Cabecera.observaciones))
                    //        _oDocVenta._Cabecera._Observacio = toAlbaven.Cabecera.observaciones;

                    //    // Dirección de envío para el pedido de venta
                    //    if (toAlbaven.Direccion != null)
                    //    {
                    //        if (loCliente._ClienteContado)
                    //        {
                    //            _oDocVenta._Cabecera._DatosContado._Nombre = toAlbaven.Direccion.nombre;
                    //            _oDocVenta._Cabecera._DatosContado._Cif = toAlbaven.Direccion.cif;
                    //            _oDocVenta._Cabecera._DatosContado._Direccion = toAlbaven.Direccion.direccion;
                    //            _oDocVenta._Cabecera._DatosContado._CodPost = toAlbaven.Direccion.codpos;
                    //            _oDocVenta._Cabecera._DatosContado._Poblacion = toAlbaven.Direccion.poblacion;
                    //            _oDocVenta._Cabecera._DatosContado._Provincia = toAlbaven.Direccion.provincia;
                    //            _oDocVenta._Cabecera._DatosContado._Pais = toAlbaven.Direccion.pais;
                    //            _oDocVenta._Cabecera._DatosContado._Email = toAlbaven.Direccion.email;
                    //            _oDocVenta._Cabecera._DatosContado._Telefono = toAlbaven.Direccion.telefono;
                    //            _oDocVenta._Cabecera._DatosContado._Save();
                    //        }
                    //        else
                    //        {
                    //            // Obtenemos un datatable con las direcciones de la ficha del cliente
                    //            DataTable loDirecciones = loCliente._Direcciones();

                    //            // Buscamos la dirección dentro del datatable
                    //            DataRow[] loRow = loDirecciones.Select(String.Format("direccion = '{0}' AND codpos = '{1}' AND poblacion = '{2}' AND provincia = '{3}'", toAlbaven.Direccion.direccion, toAlbaven.Direccion.codpos, toAlbaven.Direccion.poblacion, toAlbaven.Direccion.provincia));

                    //            if (loRow.Length > 0)
                    //            {
                    //                // Si la dirección existe, le aplicamos el número de la linea relacionado al pedido de venta
                    //                _oDocVenta._Cabecera._Env_cli = Convert.ToInt16(loRow[0]["linea"]);
                    //            }
                    //            else
                    //            {
                    //                // damos de alta la nueva dirección de envío en la ficha del cliente
                    //                Cliente.Direcciones.Direccion loDireccion = loCliente._TRelDirecciones._NewItem();
                    //                loDireccion._Nombre = toAlbaven.Direccion.nombre;
                    //                loDireccion._Direccion = toAlbaven.Direccion.direccion;
                    //                loDireccion._CodPos = toAlbaven.Direccion.codpos;
                    //                loDireccion._Poblacion = toAlbaven.Direccion.poblacion;
                    //                loDireccion._Provincia = toAlbaven.Direccion.provincia;
                    //                loDireccion._Pais = toAlbaven.Direccion.pais;
                    //                loDireccion._Telefono = toAlbaven.Direccion.telefono;
                    //                loDireccion._Tipo = (int)Cliente.Direcciones.TiposDirecciones.Envios;   // indicamos que la dirección es de envios.
                    //                // grabamos el registro
                    //                loCliente._TRelDirecciones._SaveItem(loDireccion);
                    //                //

                    //                _oDocVenta._Cabecera._Env_cli = loDireccion._Linia;
                    //            }

                    //            loDirecciones.Dispose();

                    //        }
                    //    }


                    //    if (toAlbaven.Lineas != null && toAlbaven.Lineas.Count != 0)
                    //    {
                    //        string lsCodigo = string.Empty;

                    //        foreach (var LineaAlbaran in toAlbaven.Lineas)
                    //        {
                    //            //_oLinia = _oDocVenta._AddLinea();


                    //            lsCodigo = DB.SQLValor("ARTICULO", "CODIGO", LineaAlbaran.articulo, "CODIGO").ToString();
                    //            //if (!string.IsNullOrEmpty(lsCodigo))
                    //            //{
                    //            //    _oLinia._Articulo = lsCodigo;
                    //            //    _oLinia._Talla = LineaAlbaran.talla;
                    //            //    _oLinia._Color = LineaAlbaran.color;

                    //            //    if (LineaAlbaran.lotes == null)
                    //            //        _oLinia._Unidades = LineaAlbaran.unidades;

                    //            //    # region Lotes

                    //            //    // pasamos los lotes de la linea actual
                    //            //    if (LineaAlbaran.lotes != null)
                    //            //    {

                    //            //        // creamos el objeto para los lotes de la linea del documento
                    //            //        _oLinVenDetLotes = new LinVenDet<LinVenDetLotes>();
                    //            //        _oLinVenDetLotes._Lineas = _oLinia;
                    //            //        _oLinVenDetLotes._Automatico = true;

                    //            //        // Diccionario de valores para hacer el _UpdateSilent
                    //            //        // y asi no ejecutar el set de _unidades
                    //            //        Dictionary<String, Object> ldicValores = new Dictionary<string, object>();
                    //            //        ewCampo loCampoUnidades = new ewCampo();
                    //            //        loCampoUnidades._OldVal = loCampoUnidades._NewVal = LineaAlbaran.unidades;
                    //            //        ldicValores.Add("_nUnidades", loCampoUnidades);
                    //            //        _oLinia._UpdateSilent(ldicValores);

                    //            //        // pasamos los lotes para el albaran de venta
                    //            //        foreach (clsAlbavenLineasLotes loItem in LineaAlbaran.lotes)
                    //            //        {
                    //            //            DataRow loRow = null;

                    //            //            //Lote loLote = new Lote(loItem.lote, _oLinia);
                    //            //            Lote loLote = new Lote(loItem.lote, _oLinia);

                    //            //            // Obteniendo el DataTable de la asigancion de los lotes en venta
                    //            //            DataTable ldStockLote = _Stocklote_AsignacionVentas(loLote, _oLinia, loItem.lote);

                    //            //            if (ldStockLote.Rows.Count > 0)
                    //            //            {
                    //            //                loRow = ldStockLote.Rows[0];
                    //            //                loRow["uniasig"] = loItem.unidades;
                    //            //                loRow["pesasig"] = loItem.peso;
                    //            //                //loRow["ubica"] = loItem.ubicacion;
                    //            //                loRow["asi"] = "";
                    //            //                loRow["sel"] = true;

                    //            //                _oLinVenDetLotes._lisCodigos.Add(new LinVenDetLotes(_oLinia, loRow, false));
                    //            //                int lnPos = (_oLinVenDetLotes._lisCodigos.Count - 1);

                    //            //                _oLinVenDetLotes._lisCodigos[lnPos]._Posicion = (lnPos + 1);

                    //            //            }

                    //            //            loRow = null;
                    //            //            ldStockLote.Dispose();
                    //            //        }

                    //            //    }

                    //            //    #endregion Lotes

                    //            //    // si indicamos que aplique los precios que lee desde el objeto
                    //            //    // de lo contrario, aplicará los precios calculados por Sage50
                    //            //    toAlbaven.Cabecera.precios = true;

                    //            //    if (toAlbaven.Cabecera.precios)
                    //            //    {
                    //            //        _oLinia._Precio = LineaAlbaran.precio;
                    //            //        _oLinia._Dto1 = LineaAlbaran.dto1;
                    //            //        _oLinia._Dto2 = LineaAlbaran.dto2;
                    //            //        _oLinia._Recalcular_Importe();
                    //            //    }
                    //            //}
                    //            //else
                    //            //{
                    //            //    // Es una linea de comentario
                    //            //    if (!string.IsNullOrEmpty(LineaAlbaran.definicion))
                    //            //    {
                    //            //        _oLinia._Definicion = LineaAlbaran.definicion;
                    //            //        _oLinia._TipoIva = "";
                    //            //    }
                    //            //    else
                    //            //    {
                    //            //        this._Error_Message += "El código de articulo " + LineaAlbaran.articulo + ", no existe\r\n";
                    //            //    }
                    //            //}

                    //            //if (_oLinia._Save())
                    //            //{
                    //            //    if (_oLinVenDetLotes != null)
                    //            //        _oLinVenDetLotes._Save();
                    //            //}

                    //            _oLinVenDetLotes = null;
                    //        }

                    //    }

                    //    // grabamos el pedido
                    //    _oDocVenta._Totalizar();
                    //    llOk = _oDocVenta._Save();

                    //    if (llOk)
                    //    {
                    //        toAlbaven.Cabecera.numero = _oDocVenta._Numero;
                    //        toAlbaven.Cabecera.factura = _oDocVenta._Cabecera._Factura;
                    //    }
                    //    else
                    //    {
                    //        string lsNumero = string.IsNullOrEmpty(toAlbaven.Cabecera.letra) ? "" : toAlbaven.Cabecera.letra;
                    //        lsNumero += toAlbaven.Cabecera.numero;
                    //        this._Error_Message += "No se a podido guardar el albarán de venta :" + lsNumero + "\r\n";
                    //    }
                    //}
                }
                else
                {
                    this._Error_Message += "El Código de cliente " + toAlbaven.Cabecera.cliente + ", no existe\r\n";
                }
            }
            else
            {
                if (toAlbaven.Cabecera == null)
                    this._Error_Message += "Los datos de la cabecera albarán son obligatorios\r\n";
                else
                    this._Error_Message += "Es obligatorio insertar un artículo para poder generar el albarán\r\n";
            }
            return llOk;
        }
    }
    #endregion Albarán de venta

    #region Facturas de ventas directas
    public class FacturaVentaDirecta : BaseDocument
    {
        ewDocVentaFRA _oDocVentaFra;
        ewDocVentaLinFRA _oLinia;
        LinVenDet<LinVenDetLotes> _oLinVenDetLotes;

        public Boolean _Create(clsFactuven toFactuven)
        {
            this._Error_Message = string.Empty;
            bool llOk = false, llContinue = false;

            if (toFactuven.Cabecera != null && toFactuven.Lineas != null && toFactuven.Lineas.Count != 0)
            {
                Cliente loCliente;

                _oDocVentaFra = new ewDocVentaFRA();
                _oLinia = new ewDocVentaLinFRA();

                toFactuven.Cabecera.empresa = this._Empresa;
                toFactuven.Cabecera.ejercicio = this._Ejercicio;
                toFactuven.Cabecera.letra = toFactuven.Cabecera.letra.Trim().PadLeft(2, ' ');
                toFactuven.Cabecera.numero = toFactuven.Cabecera.numero.Trim().PadLeft(10, ' ');

                // Validamos que existe código de cliente
                if (toFactuven.Direccion != null)
                {
                    // Usamos el CIF, para ubicar el primer registro que coincida en la tabla de cliente
                    if (string.IsNullOrEmpty(toFactuven.Cabecera.cliente) && !string.IsNullOrEmpty(toFactuven.Direccion.cif))
                        toFactuven.Cabecera.cliente = ReplicatedDB.SQLValor("CLIENTES", "CIF", toFactuven.Direccion.cif, "CODIGO").ToString();
                }

                // Aplicamos el codigo de clientes varios si es necesario
                toFactuven.Cabecera.cliente = (string.IsNullOrEmpty(toFactuven.Cabecera.cliente)) ? this._CliVarios : toFactuven.Cabecera.cliente;

                // Abrimos el objeto de cliente
                loCliente = new Cliente();
                loCliente._Codigo = toFactuven.Cabecera.cliente;

                // comprobamos que exista el cliente para poder crear el pedido
                if (loCliente._Existe_Registro())
                {
                    // comprobamos si el pedido ya existe
                    if (_oDocVentaFra._Existe(toFactuven.Cabecera.empresa, toFactuven.Cabecera.numero, toFactuven.Cabecera.letra))
                    {
                        // si ya existe, lo cargamos
                        _oDocVentaFra._Load(toFactuven.Cabecera.empresa, toFactuven.Cabecera.numero, toFactuven.Cabecera.letra);
                        if (_oDocVentaFra._Cabecera._Cliente != toFactuven.Cabecera.cliente)
                            this._Error_Message += "El Código del cliente del pedido es " + _oDocVentaFra._Cabecera._Cliente + ", y se esta informando un cliente es diferente " + toFactuven.Cabecera.cliente + "\r\n";
                        else
                            llContinue = true;
                    }
                    else
                    {
                        // no existe, lo creamos
                        _oDocVentaFra._New(toFactuven.Cabecera.empresa, toFactuven.Cabecera.letra, toFactuven.Cabecera.numero);
                        _oDocVentaFra._Cabecera._Cliente = toFactuven.Cabecera.cliente;
                        llContinue = true;
                    }

                    if (llContinue)
                    {
                        // validamos la forma de pago
                        string lsFPago = ReplicatedDB.SQLValor("FPAG", "CODIGO", toFactuven.Cabecera.formapago, "CODIGO").ToString();
                        if (!string.IsNullOrEmpty(lsFPago))
                            _oDocVentaFra._Cabecera._FormaPago = lsFPago;

                        if (!string.IsNullOrEmpty(toFactuven.Cabecera.observaciones))
                            _oDocVentaFra._Cabecera._Observacio = toFactuven.Cabecera.observaciones;

                        // Dirección de envío para el pedido de venta
                        if (toFactuven.Direccion != null)
                        {
                            if (loCliente._ClienteContado)
                            {
                                _oDocVentaFra._Cabecera._DatosContado._Nombre = toFactuven.Direccion.nombre;
                                _oDocVentaFra._Cabecera._DatosContado._Cif = toFactuven.Direccion.cif;
                                _oDocVentaFra._Cabecera._DatosContado._Direccion = toFactuven.Direccion.direccion;
                                _oDocVentaFra._Cabecera._DatosContado._CodPost = toFactuven.Direccion.codpos;
                                _oDocVentaFra._Cabecera._DatosContado._Poblacion = toFactuven.Direccion.poblacion;
                                _oDocVentaFra._Cabecera._DatosContado._Provincia = toFactuven.Direccion.provincia;
                                _oDocVentaFra._Cabecera._DatosContado._Pais = toFactuven.Direccion.pais;
                                _oDocVentaFra._Cabecera._DatosContado._Email = toFactuven.Direccion.email;
                                _oDocVentaFra._Cabecera._DatosContado._Telefono = toFactuven.Direccion.telefono;
                                _oDocVentaFra._Cabecera._DatosContado._Save();
                            }
                            else
                            {
                                // Obtenemos un datatable con las direcciones de la ficha del cliente
                                DataTable loDirecciones = loCliente._Direcciones();

                                // Buscamos la dirección dentro del datatable
                                DataRow[] loRow = loDirecciones.Select(String.Format("direccion = '{0}' AND codpos = '{1}' AND poblacion = '{2}' AND provincia = '{3}'", toFactuven.Direccion.direccion, toFactuven.Direccion.codpos, toFactuven.Direccion.poblacion, toFactuven.Direccion.provincia));

                                if (loRow.Length > 0)
                                {
                                    // Si la dirección existe, le aplicamos el número de la linea relacionado al pedido de venta
                                    _oDocVentaFra._Cabecera._Env_cli = Convert.ToInt16(loRow[0]["linea"]);
                                }
                                else
                                {
                                    // damos de alta la nueva dirección de envío en la ficha del cliente
                                    Cliente.Direcciones.Direccion loDireccion = loCliente._TRelDirecciones._NewItem();
                                    loDireccion._Nombre = toFactuven.Direccion.nombre;
                                    loDireccion._Direccion = toFactuven.Direccion.direccion;
                                    loDireccion._CodPos = toFactuven.Direccion.codpos;
                                    loDireccion._Poblacion = toFactuven.Direccion.poblacion;
                                    loDireccion._Provincia = toFactuven.Direccion.provincia;
                                    loDireccion._Pais = toFactuven.Direccion.pais;
                                    loDireccion._Telefono = toFactuven.Direccion.telefono;
                                    loDireccion._Tipo = (int)Cliente.Direcciones.TiposDirecciones.Envios;   // indicamos que la dirección es de envios.
                                    // grabamos el registro
                                    loCliente._TRelDirecciones._SaveItem(loDireccion);
                                    //

                                    _oDocVentaFra._Cabecera._Env_cli = loDireccion._Linia;
                                }

                                loDirecciones.Dispose();

                            }
                        }

                        if (toFactuven.Lineas != null && toFactuven.Lineas.Count != 0)
                        {
                            string lsCodigo = string.Empty;

                            foreach (var LineaFactura in toFactuven.Lineas)
                            {
                                _oLinia = _oDocVentaFra._AddLinea();


                                lsCodigo = ReplicatedDB.SQLValor("ARTICULO", "CODIGO", LineaFactura.articulo, "CODIGO").ToString();
                                if (!string.IsNullOrEmpty(lsCodigo))
                                {
                                    _oLinia._Articulo = lsCodigo;
                                    _oLinia._Talla = LineaFactura.talla;
                                    _oLinia._Color = LineaFactura.color;
                                    //_oLinia._Peso = LineaFactura.peso;
                                    //_oLinia._Cajas = LineaFactura.cajas;

                                    if (LineaFactura.lotes == null)
                                        _oLinia._Unidades = LineaFactura.unidades;


                                    // _oLinia. 

                                    #region Lotes

                                    // pasamos los lotes de la linea actual
                                    if (LineaFactura.lotes != null)
                                    {

                                        // creamos el objeto para los lotes de la linea del documento
                                        _oLinVenDetLotes = new LinVenDet<LinVenDetLotes>();
                                        _oLinVenDetLotes._Lineas = _oLinia;
                                        _oLinVenDetLotes._Automatico = true;
                                        //_oLinVenDetLotes._Load(); /// revisar si es realmente necesario cargarlas


                                        //_oLinia.__Gestion_Delegada_Ext_Unidades(LineaAlbaran.unidades);

                                        // Diccionario de valores para hacer el _UpdateSilent
                                        // y asi no ejecutar el set de _unidades
                                        Dictionary<String, Object> ldicValores = new Dictionary<string, object>();
                                        ewCampo loCampoUnidades = new ewCampo();
                                        loCampoUnidades._OldVal = loCampoUnidades._NewVal = LineaFactura.unidades;
                                        ldicValores.Add("_nUnidades", loCampoUnidades);
                                        _oLinia._UpdateSilent(ldicValores);

                                        // pasamos los lotes para el albaran de venta
                                        foreach (clsFactuvenLineasLotes loItem in LineaFactura.lotes)
                                        {
                                            DataRow loRow = null;

                                            //Lote loLote = new Lote(loItem.lote, _oLinia);
                                            Lote loLote = new Lote(loItem.lote, _oLinia);

                                            // Obteniendo el DataTable de la asigancion de los lotes en venta
                                            DataTable ldStockLote = _Stocklote_AsignacionVentas(loLote, _oLinia, loItem.lote);

                                            if (ldStockLote.Rows.Count > 0)
                                            {
                                                loRow = ldStockLote.Rows[0];
                                                loRow["uniasig"] = loItem.unidades;
                                                loRow["pesasig"] = loItem.peso;
                                                //loRow["ubica"] = loItem.ubicacion;
                                                loRow["asi"] = "";
                                                loRow["sel"] = true;

                                                _oLinVenDetLotes._lisCodigos.Add(new LinVenDetLotes(_oLinia, loRow, false));
                                                int lnPos = (_oLinVenDetLotes._lisCodigos.Count - 1);

                                                _oLinVenDetLotes._lisCodigos[lnPos]._Posicion = (lnPos + 1);

                                            }

                                            loRow = null;
                                            ldStockLote.Dispose();
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
                                    //    foreach (clsFactuvenLineasSeries loItem in LineaAlbaran.series)
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
                                    toFactuven.Cabecera.precios = true;

                                    if (toFactuven.Cabecera.precios)
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

                                        _oLinia._Precio = LineaFactura.precio;
                                        _oLinia._Dto1 = LineaFactura.dto1;
                                        _oLinia._Dto2 = LineaFactura.dto2;
                                        _oLinia._Recalcular_Importe();
                                    }

                                }
                                else
                                {
                                    // Es una linea de comentario
                                    if (!string.IsNullOrEmpty(LineaFactura.definicion))
                                    {
                                        _oLinia._Definicion = LineaFactura.definicion;
                                        _oLinia._TipoIva = "";
                                    }
                                    else
                                    {
                                        this._Error_Message += "El código de articulo " + LineaFactura.articulo + ", no existe\r\n";
                                    }

                                }


                                if (_oLinia._Save())
                                {
                                    if (_oLinVenDetLotes != null)
                                        _oLinVenDetLotes._Save();
                                }

                                _oLinVenDetLotes = null;
                            }

                        }

                        
                        _oDocVentaFra._Totalizar();

                        // grabamos la factura (sin contabilizar)
                        llOk = _oDocVentaFra._Save();

                        if (llOk)
                        {
                            toFactuven.Cabecera.numero = _oDocVentaFra._Numero;
                            toFactuven.Cabecera.factura = _oDocVentaFra._Cabecera._Factura;

                            // Para contabilizar la factura, crear la previsión, etc, etc.
                            _oDocVentaFra._Contabilizar();
                        }
                        else
                        {
                            string lsNumero = string.IsNullOrEmpty(toFactuven.Cabecera.letra) ? "" : toFactuven.Cabecera.letra;
                            lsNumero += toFactuven.Cabecera.numero;
                            this._Error_Message += "No se a podido guardar el la factura de venta :" + lsNumero + "\r\n";
                        }
                    }
                }
                else
                {
                    this._Error_Message += "El Código de cliente " + toFactuven.Cabecera.cliente + ", no existe\r\n";
                }
            }
            else
            {
                if (toFactuven.Cabecera == null)
                    this._Error_Message += "Los datos de la cabecera albarán son obligatorios\r\n";
                else
                    this._Error_Message += "Es obligatorio insertar un artículo para poder generar el albarán\r\n";
            }

            return llOk;
        }

        /// <summary>
        /// </summary>
        /// <param name="tsNumero">Numero de factura (10 digitos = (2) Serie + (8) Número)</param>
        /// <returns></returns>
        public Boolean _Exist(string tsNumero)
        {
            if (_oDocVentaFra == null)
                _oDocVentaFra = new ewDocVentaFRA();

            bool llOk = _oDocVentaFra._Existe(this._Empresa, tsNumero);

            return llOk;
        }
    }

    #endregion Facturas de ventas directas

    internal class FuncVentas
    {

    }

}
