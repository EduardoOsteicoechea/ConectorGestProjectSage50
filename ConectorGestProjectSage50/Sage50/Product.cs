using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.ComponentModel;
using System.Reflection;

//dll de sage50
using sage.ew.db;
using sage.ew.articulo;
using sage.ew.global;
using sage.ew.global.Diccionarios;
using sage.ew.functions;
using sage.ew.articulo.Clases;
using sage.ew.stocks;

// propio
using ExampleSatelite.Sage50.Datos;


namespace ExampleSatelite.Sage50.Negocio
{
    public class Product : BaseMaster
    {
        #region propiedades

        private LinkFuncSage50 _oLinkFuncs = new LinkFuncSage50();
        private Articulo _oArticulo = null;
        private int _nDigitos = Convert.ToInt32(EW_GLOBAL._GetLenCampo(KeyDiccionarioLenCampos.wn_articulo));

        #endregion propiedades

        #region constructor
        public Product()
        {
            psDb = "gestion";
            psTable = "articulo";
        }
        #endregion constructor

        public override bool _Load(ref dynamic toeProduct)
        {
            bool llOk = false;

            if(toeProduct != null)
            {
                if(!string.IsNullOrWhiteSpace(toeProduct.codigo) && toeProduct.codigo.Trim().Length <= _nDigitos)
                {

                    _oArticulo = new Articulo();
                    _oArticulo._Codigo = toeProduct.codigo;

                    toeProduct.nombre = _oArticulo._Nombre;
                    toeProduct.familia = _oArticulo._Familia;
                    toeProduct.subfamilia = _oArticulo._Subfamilia;
                    toeProduct.marca = _oArticulo._Marca;
                    toeProduct.tipo_iva = _oArticulo._TipoIVA;
                    toeProduct.ultimocoste = _oArticulo._UltimoCoste;
                    toeProduct.usaTallasColores = false; // _oArticulo._Con_TallasyColores;
                    toeProduct.usaSeries = false;  // _oArticulo._UsaSeries;
                    toeProduct.usaLotes = false; // _oArticulo._Con_Lotes(DateTime.Now);
                    toeProduct.existeregistro = _oArticulo._Existe_Registro();

                    #region precios

                    this._CreateDataTablePrecios(ref toeProduct);

                    Articulo.Precios loPrecios = _oArticulo._Precios;
                    if(loPrecios != null)
                    {
                        foreach(Articulo.Precios.Precio loPrecio in loPrecios)
                        {
                            DataRow ldrNouItem = toeProduct.precios.NewRow();
                            ldrNouItem["TARIFA"] = loPrecio._Tarifa;
                            ldrNouItem["NOMBRE"] = loPrecio._Nombre;
                            ldrNouItem["PRECIO"] = loPrecio._Pvp;
                            ldrNouItem["MONEDA"] = loPrecio._Moneda;
                            toeProduct.precios.Rows.Add(ldrNouItem);
                        }
                    }
                    else
                    {
                        toeProduct.tarifas = null;
                    }
                    #endregion precios

                    #region stocks por almacen

                    this._CreateDataTableStock(ref toeProduct);

                    Stock loStock = new Stock();
                    loStock._Fecha = DateTime.Today;
                    loStock._Articulo_Min = _oArticulo._Codigo;
                    loStock._Articulo_Max = _oArticulo._Codigo;
                    loStock._TipoCalculo = Stock.TipoCalculo.Stocks2;       //Forzar siempre Stocks2
                    loStock._GetStock();

                    DataTable ldtAlmacenes = _oArticulo._Almacenes();
                    string lcAlmacen = string.Empty, lcNombre = string.Empty;
                    foreach(DataRow ldrItem in ldtAlmacenes.Rows)
                    {
                        DataRow ldrNouItem = toeProduct.stocks.NewRow();
                        lcAlmacen = Convert.ToString(ldrItem["CODIGO"]);
                        lcNombre = Convert.ToString(ldrItem["NOMBRE"]);

                        ldrNouItem["ALMACEN"] = lcAlmacen;
                        ldrNouItem["NOMBRE"] = lcNombre;
                        ldrNouItem["STOCK"] = this._ObtenerTotal(loStock, "EXISTENCIAS", lcAlmacen);
                        ldrNouItem["DISPONIBLE"] = this._ObtenerTotal(loStock, "DISPONIBLE", lcAlmacen);
                        ldrNouItem["VIRTUAL"] = this._ObtenerTotal(loStock, "VIRTUAL", lcAlmacen);

                        toeProduct.stocks.Rows.Add(ldrNouItem);

                    }
                    #endregion stocks por almacen

                    llOk = true;
                }
                else
                { this._Error_Message += "No se a indicado el códgo del cliente o la longitud del codigo es diferente a " + _nDigitos + " digitos \r\n"; }

            }

            return llOk;
        }

        public override bool _Create(dynamic toEntity)
        {
            return true;
        }

        public override bool _Update(dynamic toEntity)
        {
            return true;
        }

        public override bool _Delete(dynamic toEntity)
        {
            return true;
        }


        private void _CreateDataTablePrecios(ref dynamic toeProduct)
        {
            if(toeProduct.precios != null)
            {
                toeProduct.precios.Clear();
                toeProduct.precios.Dispose();
            }

            toeProduct.precios = new DataTable();
            toeProduct.precios.Columns.Add("TARIFA", typeof(string)).DefaultValue = "";
            toeProduct.precios.Columns.Add("NOMBRE", typeof(string)).DefaultValue = "";
            toeProduct.precios.Columns.Add("PRECIO", typeof(decimal)).DefaultValue = 0M;
            toeProduct.precios.Columns.Add("MONEDA", typeof(string)).DefaultValue = "";
        }

        private void _CreateDataTableStock(ref dynamic toeProduct)
        {
            if(toeProduct.stocks != null)
            {
                toeProduct.stocks.Clear();
                toeProduct.stocks.Dispose();
            }

            toeProduct.stocks = new DataTable();
            toeProduct.stocks.Columns.Add("ALMACEN", typeof(string)).DefaultValue = "TOTAL";
            toeProduct.stocks.Columns.Add("NOMBRE", typeof(string)).DefaultValue = "";
            toeProduct.stocks.Columns.Add("STOCK", typeof(decimal)).DefaultValue = 0;
            toeProduct.stocks.Columns.Add("VIRTUAL", typeof(decimal)).DefaultValue = 0;
            toeProduct.stocks.Columns.Add("DISPONIBLE", typeof(decimal)).DefaultValue = 0;

        }

        private decimal _ObtenerTotal(Stock toStock, string tcTipo, string tcAlmacen)
        {
            DataTable ldtStock = new DataTable();

            switch(tcTipo)
            {
                case "EXISTENCIAS":
                    ldtStock = toStock._Existencias(true);
                    break;
                case "INICIAL":
                    ldtStock = toStock._Inicial(true);
                    break;
                case "ENTRADAS":
                    ldtStock = toStock._Entradas(true);
                    break;
                case "ENTRADAS_TRASPASO":
                    ldtStock = toStock._Entradas_Traspasadas(true);
                    break;
                case "PEDICOM":
                    ldtStock = toStock._Pedidos_Compra(true);
                    break;
                case "DEPOCOM":
                    ldtStock = toStock._Depositos_Compras(true);
                    break;
                case "SALIDAS":
                    ldtStock = toStock._Salidas(true);
                    break;
                case "SALIDAS_TRASPASO":
                    ldtStock = toStock._Salidas_Traspasadas(true);
                    break;
                case "PEDIVEN":
                    ldtStock = toStock._Pedidos_Venta(true);
                    break;
                case "DEPOVEN":
                    ldtStock = toStock._Depositos_Ventas(true);
                    break;
                case "ENTRADAS_PRODUCCION":
                    ldtStock = toStock._Entrada_Prod(true);
                    break;
                case "SALIDAS_PRODUCCION":
                    ldtStock = toStock._Salida_Prod(true);
                    break;
                case "PENDIENTE_PRODUCCION":
                    ldtStock = toStock._Pendent_Produccio(true);
                    break;
                case "EN_PRODUCCION":
                    ldtStock = toStock._En_Produccio(true);
                    break;
                case "ENTRADAS_TRANSFORM":
                    ldtStock = toStock._Entrada_Trans(true);
                    break;
                case "SALIDAS_TRANSFORM":
                    ldtStock = toStock._Salida_Trans(true);
                    break;
                case "DEVOLUCION":
                    ldtStock = toStock._Devoluciones_Prov(true);
                    break;
                case "REGULARIZACION":
                    ldtStock = toStock._Regularizaciones(true);
                    break;
                case "ALBAREGU":
                    ldtStock = toStock._Albaranes_Regularizacion(true);
                    break;
                case "VIRTUAL":
                    ldtStock = toStock._Virtual(true);
                    break;
                case "DISPONIBLE":
                    ldtStock = toStock._Disponible(true);
                    break;
            }

            if(ldtStock == null || ldtStock.Rows.Count == 0)
                return 0;

            string lcSelect = "ALMACEN = '" + tcAlmacen + "' ";
            DataRow[] ldrs = ldtStock.Select(lcSelect);

            if(ldrs.Length == 1)
                return Convert.ToDecimal(ldrs[0]["TOTAL"]);

            return 0;
        }

    }
}
