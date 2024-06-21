using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// Sage50
using sage.ew.global;
using sage.ew.global.Diccionarios;
using sage.ew.cliente;
using sage.ew.articulo;
using sage.ew.docsven;
using sage.ew.docscompra;
using sage.ew.ewbase;
using sage.ew.objetos;

//propio
using ExampleSatelite.Sage50.Negocio;
using ExampleSatelite.Sage50.Datos;
using System.IO;

namespace ExampleSatelite.Sage50.Negocio
{
    public class EjemploFacturaVentaDirectaxCode
    {
        FacturaVentaDirecta _oFraVtaDirecta = new FacturaVentaDirecta();
        public dynamic _oEntidad;
        private clsFactuvenLineas _LinFactura = new clsFactuvenLineas();

        private int _nDigitos = Convert.ToInt32(EW_GLOBAL._GetLenCampo(KeyDiccionarioLenCampos.wn_digitos));
        private int _nArticulo = Convert.ToInt32(EW_GLOBAL._GetLenCampo(KeyDiccionarioLenCampos.wn_articulo));
        private bool _llFactSer = Convert.ToBoolean(EW_GLOBAL._GetVariable("wl_Factser"));       // Trabajar con series de docummentos (albaranes)
        private bool _llSerFact = Convert.ToBoolean(EW_GLOBAL._GetVariable("wl_Serfact"));       // Trabajar con series de docummentos (facturas)

        private string _Letra = string.Empty;
        private string _Numero = string.Empty;
        private string _Almacen = EW_GLOBAL._GetVariable("wc_almacen").ToString();


        public EjemploFacturaVentaDirectaxCode()
        {
            _CrearEntidad();
        }

        private void _CrearEntidad()
        {
            _oEntidad = new clsFactuven();

            _oEntidad.Cabecera.letra = "";
            _oEntidad.Cabecera.almacen = _Almacen;
            _oEntidad.Cabecera.formapago = "00";

            if (_llSerFact)
                _oEntidad.Cabecera.letra = "SF";

        }


        private void _CrearObjetoLinea()
        {
            _LinFactura = new clsFactuvenLineas();
            _LinFactura.articulo = "";
            _LinFactura.definicion = "";
            _LinFactura.talla = "";
            _LinFactura.color = "";
            _LinFactura.unidades = 0;
            _LinFactura.precio = 0;
            _LinFactura.dto1 = 0;
            _LinFactura.dto2 = 0;
            _LinFactura.tipoiva = "";
            _LinFactura.lotes = null;
            _LinFactura.series = null;
        }

        private void _AddObjetoLinea()
        {
            int lnLinea = 0;

            // Buscamos el número maximo de la linea, para asignar el nuevo
            if (_oEntidad.Lineas != null && _oEntidad.Lineas.Count != 0)
                lnLinea = ((List<clsFactuvenLineas>)((clsFactuven)_oEntidad).Lineas).Max(x => x.linea);

            lnLinea++;

            _LinFactura.linea = lnLinea;
            _oEntidad.Lineas.Add(_LinFactura);
        }

        private void _CrearObjetoLineaLote()
        {
            _LinFactura.lotes = new List<clsFactuvenLineasLotes>();
        }


        public void _CrearEjemploFacturaDirecta()
        {
            bool llOk = false;

            _CrearEntidad();
            //_oEntidad.Cabecera.cliente = "43000104";
            _oEntidad.Cabecera.cliente = "43000002";

            // Linea1
            _CrearObjetoLinea();
            _LinFactura.articulo = "1";
            _LinFactura.definicion = "ARTICULO 1";
            _LinFactura.unidades = 2;
            _LinFactura.precio = 20;
            _LinFactura.dto1 = 0;
            _LinFactura.dto2 = 0;
            _LinFactura.tipoiva = "03";
            _LinFactura.lotes = null;
            _LinFactura.series = null;
            _AddObjetoLinea();

            // Linea2
            _CrearObjetoLinea();
            _LinFactura.articulo = "2";
            _LinFactura.definicion = "ARTICULO 2";
            _LinFactura.unidades = 6;
            _LinFactura.precio = 20;
            _LinFactura.dto1 = 0;
            _LinFactura.dto2 = 0;
            _LinFactura.tipoiva = "03";
            _LinFactura.lotes = null;
            _LinFactura.series = null;
            _AddObjetoLinea();

            // Linea2
            _CrearObjetoLinea();
            _LinFactura.articulo = "ARTLOTE";
            _LinFactura.definicion = "ARTICULO CON LOTES";
            _LinFactura.unidades = 2;
            _LinFactura.precio = 20;
            _LinFactura.dto1 = 0;
            _LinFactura.dto2 = 0;
            _LinFactura.tipoiva = "03";
            _LinFactura.lotes = null;
            _LinFactura.series = null;

            _CrearObjetoLineaLote();
            clsFactuvenLineasLotes loItemLote = new clsFactuvenLineasLotes();
            loItemLote.lote = "000002";
            loItemLote.unidades = 1;
            _LinFactura.lotes.Add(loItemLote);

            loItemLote = new clsFactuvenLineasLotes();
            loItemLote.lote = "000001";
            loItemLote.unidades = 1;
            _LinFactura.lotes.Add(loItemLote);

            _AddObjetoLinea();


            // Linea3, TYC
            _CrearObjetoLinea();
            _LinFactura.articulo = "TYC";
            _LinFactura.definicion = "ARTICULO TYC";
            _LinFactura.talla = "M ";
            _LinFactura.color = "BL";
            _LinFactura.unidades = 1;
            _LinFactura.precio = 20;
            _LinFactura.dto1 = 0;
            _LinFactura.dto2 = 0;
            _LinFactura.tipoiva = "03";
            _LinFactura.lotes = null;
            _LinFactura.series = null;
            _AddObjetoLinea();

            // Linea4, TYC
            _CrearObjetoLinea();
            _LinFactura.articulo = "TYC";
            _LinFactura.definicion = "ARTICULO TYC";
            _LinFactura.talla = "S ";
            _LinFactura.color = "NG";
            _LinFactura.unidades = 1;
            _LinFactura.precio = 16;
            _LinFactura.dto1 = 0;
            _LinFactura.dto2 = 0;
            _LinFactura.tipoiva = "03";
            _LinFactura.lotes = null;
            _LinFactura.series = null;
            _AddObjetoLinea();

            // Linea4, TYC
            _CrearObjetoLinea();
            _LinFactura.articulo = "TYC";
            _LinFactura.definicion = "ARTICULO TYC";
            _LinFactura.talla = "M ";
            _LinFactura.color = "NG";
            _LinFactura.unidades = 1;
            _LinFactura.precio = 19;
            _LinFactura.dto1 = 0;
            _LinFactura.dto2 = 0;
            _LinFactura.tipoiva = "03";
            _LinFactura.lotes = null;
            _LinFactura.series = null;
            _AddObjetoLinea();

            // Linea6
            _CrearObjetoLinea();
            _LinFactura.articulo = "";
            _LinFactura.definicion = "ARTICULO libre con la descripción del servicio prestado";
            _LinFactura.talla = "";
            _LinFactura.color = "";
            _LinFactura.unidades = 1;
            _LinFactura.precio = 500;
            _LinFactura.dto1 = 0;
            _LinFactura.dto2 = 0;
            _LinFactura.tipoiva = "03";
            _LinFactura.lotes = null;
            _LinFactura.series = null;
            _AddObjetoLinea();

            llOk = this._oFraVtaDirecta._Create(this._oEntidad);

        }


    
    }

    public class EjemploFacturaventaxCode
    {
        ewDocVentaFRA toFra = new ewDocVentaFRA();

        private string _cPathDownloadTemp = Path.Combine(Convert.ToString(EW_GLOBAL._GetVariable("wc_pathinicio")), "temp" + Path.DirectorySeparatorChar + "pruebas" + Path.DirectorySeparatorChar);
        //protected internal string _cPathDownloadTemp = Path.Combine(Convert.ToString(EW_GLOBAL._GetVariable("wc_pathinicio")), "temp" + Path.DirectorySeparatorChar + "pruebas" + Path.DirectorySeparatorChar + "gestdoctmp" + Path.DirectorySeparatorChar);

        private int _nDigitos = Convert.ToInt32(EW_GLOBAL._GetLenCampo(KeyDiccionarioLenCampos.wn_digitos));
        private int _nArticulo = Convert.ToInt32(EW_GLOBAL._GetLenCampo(KeyDiccionarioLenCampos.wn_articulo));
        private bool _llFactSer = Convert.ToBoolean(EW_GLOBAL._GetVariable("wl_Factser"));       // Trabajar con series de docummentos (albaranes)
        private bool _llSerFact = Convert.ToBoolean(EW_GLOBAL._GetVariable("wl_Serfact"));       // Trabajar con series de docummentos (facturas)

        private string _Empresa = EW_GLOBAL._GetVariable("wc_empresa").ToString();
        private string _Numero = string.Empty;
        private string _Almacen = EW_GLOBAL._GetVariable("wc_almacen").ToString();


        public EjemploFacturaventaxCode()
        { }

        public void _ImprimirFactura()
        {

            string lcIdioma = "", lcEmail = "", lcFichero = "", lcFicheroFactElectr = "", lcTemplate = "", lcPrinter = "";
            //bool llImp_Cab = false, llImp_Ser = false, llEmail_Auto = false, llImpCopEmail = false, llAgrupar = false;
            //bool llValorado = false, llIvaDesglosado = false; // Opciones de albaranes, en facturas son fijos a true los dos

            int lnAgrupar = 0;          //0 no agrupar, 1 articulo, 2 familia, 3 albaran
            bool llAgrupar = false;     //si el anterior es distinto de 0 poner true
            bool llExportar = false;    //Exportamos en alguno de los formatos
            
            lcFichero = Path.Combine(_cPathDownloadTemp, _Empresa.Trim() + _Numero.Replace(" ", "").Trim() + "_" + System.DateTime.Now.Date.Year.ToString() + System.DateTime.Now.Date.Month.ToString() + System.DateTime.Now.Date.Day.ToString() + ".PDF");
            int lnCopias = 0;

            // Factura
            _Numero = "SF       1";

            // Cargamos la Factura
            if (toFra._Load(_Empresa, _Numero))
            {
                DocPrintVentaFRA loDocPrint = toFra._DocPrint as DocPrintVentaFRA;   //Para evitar 'boxin/unboxing'
                loDocPrint._Tipo_Agrupacion = lnAgrupar;
                loDocPrint._Idioma = lcIdioma;
                if (String.IsNullOrEmpty(loDocPrint._Template))
                {
                    loDocPrint._Template = ReportTemplates._GetTemplatePredet((int)sage.ew.enumerations.TiposReport.Factuven, loDocPrint._Idioma);
                }
                else
                {
                    loDocPrint._Template = ReportTemplates._GetEquivalTemplate(loDocPrint._Template, loDocPrint._Idioma);
                }


                loDocPrint._Imprimir_Agrupado = llAgrupar;
                loDocPrint._Agrupar_Articulo = lnAgrupar == 1 ? true : false;
                loDocPrint._Imprimir_Cabecera = true;
                loDocPrint._Imprimir_Series = false;
                loDocPrint._EmailAut = false;
                //loDocPrint._Imprimir_Numero_Copias = false;
                //loDocPrint._Numero_Copias = 0;
                loDocPrint._Imprimir_Numero_Copias = false;
                loDocPrint._Numero_Copias = lnCopias;
                loDocPrint._Mensaje_Impresion = false;
                loDocPrint._Printer = loDocPrint._DefaultSystemPrinter;         //Hay que pasar la impresora con la que queremos que se imprima. Cojo la de por defecto

                if (!string.IsNullOrWhiteSpace(lcFichero))
                    loDocPrint._Ruta_Fichero = lcFichero;

                string lcFicheroFacturaElectronica = "";
                if (!string.IsNullOrWhiteSpace(lcFicheroFacturaElectronica))
                    loDocPrint._FacturaElectronica = Path.GetFileName(lcFicheroFacturaElectronica);

                loDocPrint._Vista_Preliminar = true;        // false;
                loDocPrint._Valorado = true;
                loDocPrint._IVA_Desglosado = true;
                loDocPrint._Proforma = false;

                bool llOk = toFra._DocPrint._Print();

                // Marcado de impresión, se hace por el método que incluye un _Save de la propiedad
                toFra._Printed();

                toFra._Abandonar_Documento();
            }
        
        }

    }

    public class EjemploFacturaCompraxCode
    { }
}
