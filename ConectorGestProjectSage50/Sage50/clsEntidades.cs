using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace ExampleSatelite.Sage50.Datos
{
    public class clsEntidades
    {
    }

    public class clsEntityProduct
    {
        public string codigo { get; set; } = "";
        public string nombre { get; set; } = "";
        public string familia { get; set; } = "";
        public string subfamilia { get; set; } = "";
        public string marca { get; set; } = "";
        public string tipo_iva { get; set; } = "";
        public decimal ultimocoste { get; set; } = 0M;
        public DataTable precios { get; set; } = null;
        public DataTable stocks { get; set; } = null;

        // si el artículo tiene un tratamiento especial según las siguientes características
        public bool usaTallasColores { get; set; } = false;
        public bool usaSeries { get; set; } = false;
        public bool usaLotes { get; set; } = false;
        public Boolean existeregistro { get; set; } = false;
    }

    public class clsEntityCustomer
    {
        public string codigo { get; set; } = "";
        public string pais { get; set; } = "";
        public string cif { get; set; } = "";
        public string nombre { get; set; } = "";
        public string razoncomercial { get; set; } = "";
        public bool contado { get; set; } = false;         // Cliente contado
        public string telefono { get; set; } = "";
        public string fpago { get; set; } = "";
        public string direccion { get; set; } = "";
        public string poblacion { get; set; } = "";
        public string provincia { get; set; } = "";
        public string codpos { get; set; } = "";
        public string nombrebanco { get; set; } = "";
        public string iban { get; set; } = "";
        public string swift { get; set; } = "";
        public clsEntityMandate mandato { get; set; } = null;
        public string tipo_iva { get; set; } = "";
        public string tipo_ret { get; set; } = "";
        public int modoret { get; set; } = 1; // 1 - Sobre base, 2 - Sobrefactura
        public Boolean recargo { get; set; } = false;
        public Boolean existeregistro { get; set; } = false;
    }

    public class clsEntityMandate
    {
        public string cliente { get; set; } = "";
        public string numero { get; set; } = "";
        public DateTime fecha_fin { get; set; }
        public DateTime fecha_fir { get; set; }
        public bool defecto { get; set; } = false;
        public int tipo { get; set; } = 1;
        public int tipo_pago { get; set; } = 1;
        public int numefe { get; set; } = 0;
        public int numefpro { get; set; } = 0;
    }

    public class clsEntityProvider
    {
        public string codigo { get; set; } = "";
        public string cif { get; set; } = "";
        public string nombre { get; set; } = "";
        public string razoncomercial { get; set; } = "";
        public string direccion { get; set; } = "";
        public string codpos { get; set; } = "";
        public string poblacion { get; set; } = "";
        public string provincia { get; set; } = "";
        public string pais { get; set; } = "";
        public string fpago { get; set; } = "";
        public string nombrebanco { get; set; } = "";
        public string IBAN { get; set; } = "";
        public string Swift { get; set; } = "";
        public string tipo_iva { get; set; } = "";
        public string tipo_ret { get; set; } = "";
        public int modoret { get; set; } = 1; // 1 - Sobre base, 2 - Sobrefactura
        public Boolean recargo { get; set; } = false;
    }

    public class clsBankAccount
    {
        private string Iban { get; set; } = "";
        public string tipocta { get; set; } = "IBAN";
        public string nombre { get; set; } = "Nombre Banco";
        public string IBAN
        {
            get
            {
                return this.Iban;
            }
            set
            {
                if (!String.IsNullOrWhiteSpace(value))
                {
                    this.Iban = value;
                    this.iban = this.Iban.Substring(0, 4).Trim();
                    this.cuentaiban = this.Iban.Substring(4).Trim();

                    this.codban = this.cuentaiban.Substring(0, 4).Trim();
                    this.succur = this.cuentaiban.Substring(4, 4).Trim();
                    this.digcon = this.cuentaiban.Substring(8, 2).Trim();
                    this.ctacuenta = this.cuentaiban.Substring(10, 10).Trim();
                }
            }
        }
        public string iban { get; set; } = "";
        public string cuentaiban { get; set; } = "";
        public string codban { get; set; } = "";
        public string succur { get; set; } = "";
        public string digcon { get; set; } = "";
        public string ctacuenta { get; set; } = "";
    }

    public class clsFactura
    {
        public string grupo { get; set; } = "";
        public string empresa { get; set; } = "";
        public string factura { get; set; } = "";
        public DateTime fecha { get; set; }
        public string cif { get; set; } = "";
        public string nombre { get; set; } = "";
        public string cuenta { get; set; } = "";
        public clsContrapartida[] contrapartidas { get; set; }
        public string definicion { get; set; } = "";
        public clsIva[] ivas { get; set; }
        public decimal pronto { get; set; } = 0;
        public string fpago { get; set; } = "";
        public int operacion { get; set; } = 0;
        public string refcatastral { get; set; } = "";
        public string IBAN { get; set; } = "";
    }

    public class clsContrapartida
    {
        public string cuenta { get; set; } = "";
        public decimal importe { get; set; } = 0;
        public string codigonivel1 { get; set; } = "";
        public string nombrenivel1 { get; set; } = "";
        public string codigonivel2 { get; set; } = "";
        public string nombrenivel2 { get; set; } = "";
    }

    public class clsIva
    {
        public string codigo { get; set; } = "";
        public decimal importe { get; set; } = 0;
    }

    public class clsDireccionEnvio
    {
        public string cif { get; set; } = "";
        public string nombre { get; set; } = "";
        public string direccion { get; set; } = "";
        public string poblacion { get; set; } = "";
        public string provincia { get; set; } = "";
        public string codpos { get; set; } = "";
        public string pais { get; set; } = "034";
        public string email { get; set; } = "";
        public string telefono { get; set; } = "";
        public string contacto { get; set; } = "";
    }


    #region entidades para el presupuesto de ventas

    public class clsPresuven
    {
        public clsPresuvenCabecera Cabecera { get; set; } = new clsPresuvenCabecera();
        public List<clsPresuvenLineas> Lineas { get; set; } = new List<clsPresuvenLineas>();
        public clsDireccionEnvio Direccion { get; set; } = new clsDireccionEnvio();
    }

    public class clsPresuvenCabecera
    {
        public string ejercicio { get; set; } = "";
        public string empresa { get; set; } = "";
        public string numero { get; set; } = "";
        public string letra { get; set; } = "";
        public DateTime fecha { get; set; } = DateTime.Today;
        public string cliente { get; set; } = "";
        public bool futuro { get; set; } = false;
        public string tarifa { get; set; } = "";
        public string vendedor { get; set; } = "";
        public string nombre { get; set; } = "";
        public string almacen { get; set; } = "";
        public string formapago { get; set; } = "";
        public string entrega { get; set; } = "";
        public string comentario { get; set; } = "";
        public string observaciones { get; set; } = "";
        public string refercli { get; set; } = "";
        public int enc_cli { get; set; } = -1;
        public bool precios { get; set; } = false;
        public bool traspasado { get; set; } = false;
        public bool cancelado { get; set; } = false;
    }

    public class clsPresuvenLineas
    {
        public string articulo { get; set; } = "";
        public string definicion { get; set; } = "";
        public string talla { get; set; } = "";
        public string color { get; set; } = "";
        public decimal unidades { get; set; } = 0;
        public decimal cajas { get; set; } = 0;
        public decimal peso { get; set; } = 0;
        public decimal precio { get; set; } = 0;
        public decimal dto1 { get; set; } = 0;
        public decimal dto2 { get; set; } = 0;
        public int linea { get; set; } = 0;
        public string tipoiva { get; set; } = "";

    }


    #endregion entidades para el presupuesto de ventas

    #region entidades para el pedido de ventas

    public class clsPediven
    {
        public clsPedivenCabecera Cabecera { get; set; } = new clsPedivenCabecera();
        public List<clsPedivenLineas> Lineas { get; set; } = new List<clsPedivenLineas>();
        public clsDireccionEnvio Direccion { get; set; } = new clsDireccionEnvio();
    }

    public class clsPedivenCabecera
    {
        public string ejercicio { get; set; } = "";
        public string empresa { get; set; } = "";
        public string numero { get; set; } = "";
        public string letra { get; set; } = "";
        public DateTime fecha { get; set; } = DateTime.Today;
        public string cliente { get; set; } = "";
        public string nombre { get; set; } = "";
        public string almacen { get; set; } = "";
        public string formapago { get; set; } = "";
        public string entrega { get; set; } = "";
        public string comentario { get; set; } = "";
        public string observaciones { get; set; } = "";
        public string refercli { get; set; } = "";
        public int enc_cli { get; set; } = -1;
        public bool precios { get; set; } = false;
        public bool traspasado { get; set; } = false;
        public bool cancelado { get; set; } = false;
    }

    public class clsPedivenLineas
    {
        public string articulo { get; set; } = "";
        public string definicion { get; set; } = "";
        public string talla { get; set; } = "";
        public string color { get; set; } = "";
        public decimal unidades { get; set; } = 0;
        public decimal cajas { get; set; } = 0;
        public decimal peso { get; set; } = 0;
        public decimal precio { get; set; } = 0;
        public decimal dto1 { get; set; } = 0;
        public decimal dto2 { get; set; } = 0;
        public int linea { get; set; } = 0;
        public string tipoiva { get; set; } = "";
        public List<clsPedivenLineasLotes> lotes { get; set; } = null;
    }

    public class clsPedivenLineasLotes
    {
        public string lote { get; set; } = "";
        public decimal unidades { get; set; } = 0;
    }

    //public class Ent_LineasPedivenList
    //{
    //    public string empresa;
    //    public string numero;
    //    public string letra;
    //    public string fecha;
    //    public string cliente;
    //    public string nombrecliente;
    //    public string ruta;
    //    public string entrega;
    //    public string comentario;
    //    public string refercli;
    //    public string observaciones;
    //    public string nombreruta;
    //    public string articulo;
    //    public string nombrearticulo;
    //    public string definicion;
    //    public decimal unidades;
    //    public decimal servidas;
    //    public int linea;
    //    public List<Ent_AlbaranList> albaranes;
    //}

    #endregion entidades para el pedido de ventas

    #region entidades para albaranes de ventas

    public class clsAlbaven
    {
        public clsAlbavenCabecera Cabecera { get; set; } = new clsAlbavenCabecera();
        public List<clsAlbavenLineas> Lineas { get; set; } = new List<clsAlbavenLineas>();
        public clsDireccionEnvio Direccion { get; set; } = new clsDireccionEnvio();
    }

    public class clsAlbavenCabecera
    {
        public string ejercicio { get; set; } = "";
        public string empresa { get; set; } = "";
        public string numero { get; set; } = "";
        public string letra { get; set; } = "";
        public DateTime fecha { get; set; } = DateTime.Today;
        public string cliente { get; set; } = "";
        public string nombre { get; set; } = "";
        public string almacen { get; set; } = "";
        public string formapago { get; set; } = "";
        public string entrega { get; set; } = "";
        public string comentario { get; set; } = "";
        public string observaciones { get; set; } = "";
        public string factura { get; set; } = "";
        public int enc_cli { get; set; } = -1;
        public bool precios { get; set; } = false;
        public bool facturado { get; set; } = false;
        public bool esfacturable { get; set; } = false;

    }

    public class clsAlbavenLineas
    {
        public string articulo { get; set; } = "";
        public string definicion { get; set; } = "";
        public string talla { get; set; } = "";
        public string color { get; set; } = "";
        public decimal unidades { get; set; } = 0;
        public decimal cajas { get; set; } = 0;
        public decimal peso { get; set; } = 0;
        public decimal precio { get; set; } = 0;
        public decimal dto1 { get; set; } = 0;
        public decimal dto2 { get; set; } = 0;
        public int linea { get; set; } = 0;
        public string tipoiva { get; set; } = "";
        public List<clsAlbavenLineasLotes> lotes { get; set; } = null;
        public List<clsAlbavenLineasSeries> series { get; set; } = null;
    }

    public class clsAlbavenLineasLotes
    {
        public string ubicacion { get; set; } = "";
        public string lote { get; set; } = "";
        public decimal unidades { get; set; } = 0;
        public decimal peso { get; set; } = 0;
        public string talla { get; set; } = "";
        public string color { get; set; } = "";
    }

    public class clsAlbavenLineasSeries
    {
        public string serie { get; set; } = "";
        public decimal unidades { get; set; } = 0;
        public decimal peso { get; set; } = 0;
    }


    #endregion entidades para  albaranes de ventas

    #region entidades para factura de ventas - directas

    public class clsFactuven
    {
        public clsFactuvenCabecera Cabecera { get; set; } = new clsFactuvenCabecera();
        public List<clsFactuvenLineas> Lineas { get; set; } = new List<clsFactuvenLineas>();
        public clsDireccionEnvio Direccion { get; set; } = new clsDireccionEnvio();
    }

    public class clsFactuvenCabecera
    {
        public string ejercicio { get; set; } = "";
        public string empresa { get; set; } = "";
        public string numero { get; set; } = "";
        public string letra { get; set; } = "";
        public DateTime fecha { get; set; } = DateTime.Today;
        public string cliente { get; set; } = "";
        public string nombre { get; set; } = "";
        public string almacen { get; set; } = "";
        public string formapago { get; set; } = "";
        public string entrega { get; set; } = "";
        public string comentario { get; set; } = "";
        public string observaciones { get; set; } = "";
        public string factura { get; set; } = "";
        public int enc_cli { get; set; } = -1;
        public bool precios { get; set; } = false;
        public bool facturado { get; set; } = false;
        public bool esfacturable { get; set; } = false;

    }

    public class clsFactuvenLineas
    {
        public string articulo { get; set; } = "";
        public string definicion { get; set; } = "";
        public string talla { get; set; } = "";
        public string color { get; set; } = "";
        public decimal unidades { get; set; } = 0;
        public decimal cajas { get; set; } = 0;
        public decimal peso { get; set; } = 0;
        public decimal precio { get; set; } = 0;
        public decimal dto1 { get; set; } = 0;
        public decimal dto2 { get; set; } = 0;
        public int linea { get; set; } = 0;
        public string tipoiva { get; set; } = "";
        public List<clsFactuvenLineasLotes> lotes { get; set; } = null;
        public List<clsFactuvenLineasSeries> series { get; set; } = null;
    }

    public class clsFactuvenLineasLotes
    {
        public string ubicacion { get; set; } = "";
        public string lote { get; set; } = "";
        public decimal unidades { get; set; } = 0;
        public decimal peso { get; set; } = 0;
        public string talla { get; set; } = "";
        public string color { get; set; } = "";
    }

    public class clsFactuvenLineasSeries
    {
        public string serie { get; set; } = "";
        public decimal unidades { get; set; } = 0;
        public decimal peso { get; set; } = 0;
    }


    #endregion entidades para factura de ventas - directas


    #region entidades para el pedido de compra

    public class clsPedicom
    {
        public clsPedicomCabecera Cabecera { get; set; } = new clsPedicomCabecera();
        public List<clsPedicomLineas> Lineas { get; set; } = new List<clsPedicomLineas>();
        public clsDireccionEnvio Direccion { get; set; } = new clsDireccionEnvio();
    }

    public class clsPedicomCabecera
    {
        public string ejercicio { get; set; } = "";
        public string empresa { get; set; } = "";
        public string numero { get; set; } = "";

        public DateTime fecha { get; set; } = DateTime.Today;
        public string proveedor { get; set; } = "";
        public string nombre { get; set; } = "";
        public string almacen { get; set; } = "";
        public string formapago { get; set; } = "";
        public string entrega { get; set; } = "";
        public string comentario { get; set; } = "";
        public string observaciones { get; set; } = "";

        public int env_pro { get; set; } = -1;
        public bool precios { get; set; } = false;
        public bool traspasado { get; set; } = false;
        public bool cancelado { get; set; } = false;
    }

    public class clsPedicomLineas
    {
        public string articulo { get; set; } = "";
        public string definicion { get; set; } = "";
        public string talla { get; set; } = "";
        public string color { get; set; } = "";
        public decimal unidades { get; set; } = 0;
        public decimal cajas { get; set; } = 0;
        public decimal peso { get; set; } = 0;
        public decimal precio { get; set; } = 0;
        public decimal dto1 { get; set; } = 0;
        public decimal dto2 { get; set; } = 0;
        public int linea { get; set; } = 0;
        public string tipoiva { get; set; } = "";
        public List<clsPedicomLineasLotes> lotes { get; set; } = null;
    }

    public class clsPedicomLineasLotes
    {
        public string lote { get; set; } = "";
        public decimal unidades { get; set; } = 0;
    }

    #endregion entidades para el pedido de compra


    #region entidades para albaranes de compras

    public class clsAlbacom
    {
        public clsAlbacomCabecera Cabecera { get; set; } = new clsAlbacomCabecera();
        public List<clsAlbacomLineas> Lineas { get; set; } = new List<clsAlbacomLineas>();
    }

    public class clsAlbacomCabecera
    {
        public string ejercicio { get; set; } = "";
        public string empresa { get; set; } = "";
        public string numero { get; set; } = "";
        public string letra { get; set; } = "";
        public DateTime fecha { get; set; } = DateTime.Today;
        public string proveedor { get; set; } = "";
        public string nombre { get; set; } = "";
        public string almacen { get; set; } = "";
        public string formapago { get; set; } = "";
        public string entrega { get; set; } = "";
        public string comentario { get; set; } = "";
        public string observaciones { get; set; } = "";
        public string factura { get; set; } = "";
        public int env_pro { get; set; } = -1;
        public bool precios { get; set; } = false;
        public bool facturado { get; set; } = false;
        public bool esfacturable { get; set; } = false;

    }

    public class clsAlbacomLineas
    {
        public string articulo { get; set; } = "";
        public string definicion { get; set; } = "";
        public string talla { get; set; } = "";
        public string color { get; set; } = "";
        public decimal unidades { get; set; } = 0;
        public decimal cajas { get; set; } = 0;
        public decimal peso { get; set; } = 0;
        public decimal precio { get; set; } = 0;
        public decimal dto1 { get; set; } = 0;
        public decimal dto2 { get; set; } = 0;
        public int linea { get; set; } = 0;
        public string tipoiva { get; set; } = "";
        public List<clsAlbacomLineasLotes> lotes { get; set; } = null;
        public List<clsAlbacomLineasSeries> series { get; set; } = null;
    }

    public class clsAlbacomLineasLotes
    {
        public string ubicacion { get; set; } = "";
        public string lote { get; set; } = "";
        public decimal unidades { get; set; } = 0;
        public decimal peso { get; set; } = 0;
        public string talla { get; set; } = "";
        public string color { get; set; } = "";
    }

    public class clsAlbacomLineasSeries
    {
        public string serie { get; set; } = "";
        public decimal unidades { get; set; } = 0;
        public decimal peso { get; set; } = 0;
    }


    #endregion entidades para  albaranes de compras

}

