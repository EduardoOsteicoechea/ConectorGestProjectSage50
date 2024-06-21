using ConectorGestProjectSage50.Sage50;
using ConectorGestProjectSage50.UI.Styles;
using ExampleSatelite.Sage50.Datos;
using ExampleSatelite.Sage50.Negocio;
using sage.ew.cliente;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace ConectorGestProjectSage50.UI
{
	public class MainWindow : System.Windows.Forms.Form
	{
		public MainWindow()
        {
            EOStyles.ScreenRectangle = Screen.GetBounds(this);
            EOStyles.ScreenWidth = EOStyles.ScreenRectangle.Width;
            EOStyles.ScreenHeight = EOStyles.ScreenRectangle.Height;
            EOStyles.MainFormWidth = this.Width = Convert.ToInt32(Math.Round(EOStyles.ScreenWidth * .65));
            EOStyles.MainFormHeight = this.Height = Convert.ToInt32(Math.Round(EOStyles.ScreenHeight * .73));


            MessageBox.Show("MainWindow()");

            //var aa = new CreateSalesAlbaran();
            //aa._CrearEjemploAlbaran();
            //MessageBox.Show(aa._oEntidad.Cabecera.cliente);

            var clienteBase = new clsEntityCustomer();
            //clienteBase.codigo = "wc_cliente";
            clienteBase.codigo = "wl_cliente";
            clienteBase.nombre = "Eduardo";
            clienteBase.pais = "Venezuela";
            //clienteBase.contado = true;
            clienteBase.contado = false;

            MessageBox.Show("BEFORE:\n\nvar cliente = new Customer();)");
            var cliente = new Customer();

            MessageBox.Show("BEFORE:\n\ncliente._Create(dynamic toeCustomer)");
            cliente._Create(clienteBase);

            StringBuilder sb = new StringBuilder();
            sb.Append("Object Type: " + clienteBase.GetType().Name + "\n");
            foreach(var property in clienteBase.GetType().GetProperties())
            {
                sb.Append(property.Name + ": " + property.GetValue(clienteBase) + "\n");
            };


            MessageBox.Show(sb.ToString());


            //this.ShowDialog();
		}
	}
}
