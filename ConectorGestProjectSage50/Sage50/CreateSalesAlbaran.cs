using sage.ew.global.Diccionarios;
using sage.ew.global;
using System;
using System.Collections.Generic;
using System.Linq;

using ExampleSatelite.Sage50.Negocio;
using ExampleSatelite.Sage50.Datos;
using System.Windows.Forms;
using ConectorGestProjectSage50.Sage50.ReverseEngeniering;


namespace ConectorGestProjectSage50.Sage50
{
    public class CreateSalesAlbaran
    {
        AlbaranDeVenta _oAlbaranVenta = new ConectorGestProjectSage50.Sage50.ReverseEngeniering.AlbaranDeVenta();
        public dynamic _oEntidad;
        private clsAlbavenLineas _LinAlbaran = new clsAlbavenLineas();

        //private int _nDigitos = Convert.ToInt32(EW_GLOBAL._GetLenCampo(KeyDiccionarioLenCampos.wn_digitos));
        //private int _nArticulo = Convert.ToInt32(EW_GLOBAL._GetLenCampo(KeyDiccionarioLenCampos.wn_articulo));
        private int _nDigitos = 10;
        private int _nArticulo = 11;

        private bool _llFactSer = Convert.ToBoolean(EW_GLOBAL._GetVariable("wl_Factser"));       // Trabajar con series de docummentos (albaranes)
        private bool _llSerFact = Convert.ToBoolean(EW_GLOBAL._GetVariable("wl_Serfact"));       // Trabajar con series de docummentos (facturas)

        private string _Letra = string.Empty;
        private string _Numero = string.Empty;
        private string _Almacen = EW_GLOBAL._GetVariable("wc_almacen").ToString();

        public CreateSalesAlbaran()
        {
            MessageBox.Show("CreateSalesAlbaran()");
            _CrearEntidad();
        }

        public void _CrearEjemploAlbaran()
        {
            bool llOk = false;

            _CrearEntidad();
            //_oEntidad.Cabecera.cliente = "43000104";
            _oEntidad.Cabecera.cliente = "43000002";


            MessageBox.Show("Before _CrearObjetoLinea()");
            // Linea1
            _CrearObjetoLinea();
            _LinAlbaran.articulo = "1";
            _LinAlbaran.definicion = "ARTICULO 1";
            _LinAlbaran.unidades = 2;
            _LinAlbaran.precio = 20;
            _LinAlbaran.dto1 = 0;
            _LinAlbaran.dto2 = 0;
            _LinAlbaran.tipoiva = "03";
            _LinAlbaran.lotes = null;
            _LinAlbaran.series = null;
            _AddObjetoLinea();

            // Linea2
            _CrearObjetoLinea();
            _LinAlbaran.articulo = "2";
            _LinAlbaran.definicion = "ARTICULO 2";
            _LinAlbaran.unidades = 6;
            _LinAlbaran.precio = 20;
            _LinAlbaran.dto1 = 0;
            _LinAlbaran.dto2 = 0;
            _LinAlbaran.tipoiva = "03";
            _LinAlbaran.lotes = null;
            _LinAlbaran.series = null;
            _AddObjetoLinea();

            // Linea2
            _CrearObjetoLinea();
            _LinAlbaran.articulo = "ARTLOTE";
            _LinAlbaran.definicion = "ARTICULO CON LOTES";
            _LinAlbaran.unidades = 2;
            _LinAlbaran.precio = 20;
            _LinAlbaran.dto1 = 0;
            _LinAlbaran.dto2 = 0;
            _LinAlbaran.tipoiva = "03";
            _LinAlbaran.lotes = null;
            _LinAlbaran.series = null;

            _CrearObjetoLineaLote();
            clsAlbavenLineasLotes loItemLote = new clsAlbavenLineasLotes();
            loItemLote.lote = "000091";
            loItemLote.unidades = 1;
            _LinAlbaran.lotes.Add(loItemLote);

            loItemLote = new clsAlbavenLineasLotes();
            loItemLote.lote = "000092";
            loItemLote.unidades = 1;
            _LinAlbaran.lotes.Add(loItemLote);
            _AddObjetoLinea();


            // Linea3, TYC
            _CrearObjetoLinea();
            _LinAlbaran.articulo = "TYC";
            _LinAlbaran.definicion = "ARTICULO TYC";
            _LinAlbaran.talla = "M ";
            _LinAlbaran.color = "BL";
            _LinAlbaran.unidades = 1;
            _LinAlbaran.precio = 20;
            _LinAlbaran.dto1 = 0;
            _LinAlbaran.dto2 = 0;
            _LinAlbaran.tipoiva = "03";
            _LinAlbaran.lotes = null;
            _LinAlbaran.series = null;
            _AddObjetoLinea();

            // Linea4, TYC
            _CrearObjetoLinea();
            _LinAlbaran.articulo = "TYC";
            _LinAlbaran.definicion = "ARTICULO TYC";
            _LinAlbaran.talla = "S ";
            _LinAlbaran.color = "NG";
            _LinAlbaran.unidades = 1;
            _LinAlbaran.precio = 16;
            _LinAlbaran.dto1 = 0;
            _LinAlbaran.dto2 = 0;
            _LinAlbaran.tipoiva = "03";
            _LinAlbaran.lotes = null;
            _LinAlbaran.series = null;
            _AddObjetoLinea();

            // Linea5, TYC
            _CrearObjetoLinea();
            _LinAlbaran.articulo = "TYC";
            _LinAlbaran.definicion = "ARTICULO TYC";
            _LinAlbaran.talla = "M ";
            _LinAlbaran.color = "NG";
            _LinAlbaran.unidades = 1;
            _LinAlbaran.precio = 19;
            _LinAlbaran.dto1 = 0;
            _LinAlbaran.dto2 = 0;
            _LinAlbaran.tipoiva = "03";
            _LinAlbaran.lotes = null;
            _LinAlbaran.series = null;
            _AddObjetoLinea();


            MessageBox.Show("BEFORE\n\nllOk = this._oAlbaranVenta._Create(this._oEntidad)");
            llOk = this._oAlbaranVenta._Create(this._oEntidad);
            MessageBox.Show("AFTER\n\nllOk = this._oAlbaranVenta._Create(this._oEntidad)");
        }



        private void _CrearEntidad()
        {
            _oEntidad = new clsAlbaven(); // Albarán de Venta Sage 50 = Facturas emitidas de Gest project
            _oEntidad.Cabecera.letra = "";
            _oEntidad.Cabecera.almacen = _Almacen;
            _oEntidad.Cabecera.formapago = "00";

            if(_llFactSer)
                _oEntidad.Cabecera.letra = "SF";
            MessageBox.Show("_CrearEntidad()");

        }

        private void _CrearObjetoLinea()
        {
            MessageBox.Show("inside _CrearObjetoLinea()");
            _LinAlbaran = new clsAlbavenLineas();
            _LinAlbaran.articulo = "";
            _LinAlbaran.definicion = "";
            _LinAlbaran.talla = "";
            _LinAlbaran.color = "";
            _LinAlbaran.unidades = 0;
            _LinAlbaran.precio = 0;
            _LinAlbaran.dto1 = 0;
            _LinAlbaran.dto2 = 0;
            _LinAlbaran.tipoiva = "";
            _LinAlbaran.lotes = null;
            _LinAlbaran.series = null;
        }

        private void _AddObjetoLinea()
        {
            MessageBox.Show("inside _AddObjetoLinea()");
            int lnLinea = 0;

            // Buscamos el número maximo de la linea, para asignar el nuevo
            if(_oEntidad.Lineas != null && _oEntidad.Lineas.Count != 0)
                lnLinea = ((List<clsAlbavenLineas>)((clsAlbaven)_oEntidad).Lineas).Max(x => x.linea);

            lnLinea++;

            _LinAlbaran.linea = lnLinea;
            _oEntidad.Lineas.Add(_LinAlbaran);
        }

        private void _CrearObjetoLineaLote()
        {
            _LinAlbaran.lotes = new List<clsAlbavenLineasLotes>();
        }
    }

    public class EjemploAlbacomxCode
    {
        AlbaranCompra _oAlbaranCompra = new AlbaranCompra();
        public dynamic _oEntidad;
        private clsAlbacomLineas _LinAlbaran = new clsAlbacomLineas();

        //private int _nDigitos = Convert.ToInt32(sage.ew.global.EW_GLOBAL._GetLenCampo(KeyDiccionarioLenCampos.wn_digitos));
        //private int _nArticulo = Convert.ToInt32(sage.ew.global.EW_GLOBAL._GetLenCampo(KeyDiccionarioLenCampos.wn_articulo));

        private int _nDigitos = 10;
        private int _nArticulo = 11;

        private string _Letra = string.Empty;
        private string _Numero = string.Empty;
        private string _Almacen = EW_GLOBAL._GetVariable("wc_almacen").ToString();

        public EjemploAlbacomxCode()
        {
            _CrearEntidad();
        }

        public void _CrearEjemploAlbaran()
        {
            bool llOk = false;

            _CrearEntidad();
            _oEntidad.Cabecera.proveedor = "41000001";

            // Linea1
            _CrearObjetoLinea();
            _LinAlbaran.articulo = "1";
            _LinAlbaran.definicion = "ARTICULO 1";
            _LinAlbaran.unidades = 2;
            _LinAlbaran.precio = 20;
            _LinAlbaran.dto1 = 0;
            _LinAlbaran.dto2 = 0;
            _LinAlbaran.tipoiva = "03";
            _LinAlbaran.lotes = null;
            _LinAlbaran.series = null;
            _AddObjetoLinea();

            // Linea2
            _CrearObjetoLinea();
            _LinAlbaran.articulo = "2";
            _LinAlbaran.definicion = "ARTICULO 2";
            _LinAlbaran.unidades = 6;
            _LinAlbaran.precio = 20;
            _LinAlbaran.dto1 = 0;
            _LinAlbaran.dto2 = 0;
            _LinAlbaran.tipoiva = "03";
            _LinAlbaran.lotes = null;
            _LinAlbaran.series = null;
            _AddObjetoLinea();

            // Linea2
            _CrearObjetoLinea();
            _LinAlbaran.articulo = "ARTLOTE";
            _LinAlbaran.definicion = "ARTICULO CON LOTES";
            _LinAlbaran.unidades = 20;
            _LinAlbaran.precio = 20;
            _LinAlbaran.dto1 = 0;
            _LinAlbaran.dto2 = 0;
            _LinAlbaran.tipoiva = "03";
            _LinAlbaran.lotes = null;
            _LinAlbaran.series = null;

            _CrearObjetoLineaLote();
            clsAlbacomLineasLotes loItemLote = new clsAlbacomLineasLotes();
            loItemLote.lote = "000093";
            loItemLote.unidades = 10;
            _LinAlbaran.lotes.Add(loItemLote);

            loItemLote = new clsAlbacomLineasLotes();
            loItemLote.lote = "000091";
            loItemLote.unidades = 5;
            _LinAlbaran.lotes.Add(loItemLote);

            loItemLote = new clsAlbacomLineasLotes();
            loItemLote.lote = "000092";
            loItemLote.unidades = 5;
            _LinAlbaran.lotes.Add(loItemLote);

            _AddObjetoLinea();

            llOk = this._oAlbaranCompra._Create(this._oEntidad);

        }

        private void _CrearEntidad()
        {
            _oEntidad = new clsAlbacom();
            //_oEntidad.Cabecera.letra = "SF";
            _oEntidad.Cabecera.almacen = _Almacen;
            _oEntidad.Cabecera.formapago = "00";
        }

        private void _CrearObjetoLinea()
        {
            _LinAlbaran = new clsAlbacomLineas();
            _LinAlbaran.articulo = "";
            _LinAlbaran.definicion = "";
            _LinAlbaran.unidades = 0;
            _LinAlbaran.precio = 0;
            _LinAlbaran.dto1 = 0;
            _LinAlbaran.dto2 = 0;
            _LinAlbaran.tipoiva = "";
            _LinAlbaran.lotes = null;
            _LinAlbaran.series = null;
        }

        private void _AddObjetoLinea()
        {
            int lnLinea = 0;

            // Buscamos el número maximo de la linea, para asignar el nuevo
            if(_oEntidad.Lineas != null && _oEntidad.Lineas.Count != 0)
                lnLinea = ((List<clsAlbacomLineas>)((clsAlbacom)_oEntidad).Lineas).Max(x => x.linea);

            lnLinea++;

            _LinAlbaran.linea = lnLinea;
            _oEntidad.Lineas.Add(_LinAlbaran);
        }

        private void _CrearObjetoLineaLote()
        {
            _LinAlbaran.lotes = new List<clsAlbacomLineasLotes>();
        }
    }
}
