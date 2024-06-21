using sage.ew.db;
using sage.ew.global;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace ConectorGestProjectSage50.Sage50.ReverseEngeniering
{
    internal static class Replicated_FUNCTIONS
    {

        private static Dictionary<string, bool> Nivel3Cliente
        {
            get
            {
                Nivel3Cuentas();
                object obj = EW_GLOBAL._GetVariable("wo_Nivel3Clientes");
                if(obj != null)
                {
                    return (Dictionary<string, bool>)obj;
                }

                return null;
            }
        }
        private static void Nivel3Cuentas()
        {
            object obj = EW_GLOBAL._GetVariable("wo_Nivel3Clientes");
            object obj2 = EW_GLOBAL._GetVariable("wo_Nivel3Proveedores");
            if(obj != null && obj2 != null)
            {
                return;
            }

            DataTable dtTabla = new DataTable();
            if(ReplicatedDB.SQLExec("select codigo, cliente, proveedor from " + ReplicatedDB.SQLDatabase("nivel3") + " where cliente= " + ReplicatedDB.SQLTrue() + " or proveedor = " + ReplicatedDB.SQLTrue(), ref dtTabla))
            {
                Dictionary<string, bool> valor = (from r in dtTabla.AsEnumerable()
                                                  where r.Field<bool>("cliente")
                                                  select r).ToDictionary((DataRow r) => r.Field<string>("codigo"), (DataRow r) => r.Field<bool>("cliente"));
                Dictionary<string, bool> valor2 = (from r in dtTabla.AsEnumerable()
                                                   where r.Field<bool>("proveedor")
                                                   select r).ToDictionary((DataRow r) => r.Field<string>("codigo"), (DataRow r) => r.Field<bool>("proveedor"));
                EW_GLOBAL.ValorEnClave_VarGlob("wo_Nivel3Clientes", valor);
                EW_GLOBAL.ValorEnClave_VarGlob("wo_Nivel3Proveedores", valor2);
            }

            _DisposeDatatable(ref dtTabla);
        }

        public static void _DisposeDatatable(ref DataTable tdtDatatable)
        {
            if(tdtDatatable != null && tdtDatatable.Rows != null)
            {
                tdtDatatable.Rows.Clear();
                if(tdtDatatable.Constraints != null && tdtDatatable.Constraints.Count > 0)
                {
                    tdtDatatable.Constraints.Clear();
                }

                tdtDatatable.Columns.Clear();
                tdtDatatable.Dispose();
                tdtDatatable = null;
            }
        }

        public static bool _Es_Cliente(string tcCodigo)
        {
            if(!string.IsNullOrWhiteSpace(tcCodigo))
            {
                if(CompararPrefijoConClaveDeLaGlobal(tcCodigo, "wc_cliente") || CompararPrefijoConClaveDeLaGlobal(tcCodigo, "wc_deudor"))
                {
                    return true;
                }

                if(tcCodigo.TrimEnd().Length >= 3)
                {
                    bool flag = false;
                    if(Nivel3Cliente != null)
                    {
                        return Nivel3Cliente.ContainsKey(tcCodigo.Substring(0, 3));
                    }

                    return Convert.ToBoolean(ReplicatedDB.SQLValor("NIVEL3", "CODIGO", tcCodigo.Substring(0, 3), "CLIENTE"));
                }

                if(tcCodigo.Trim().Length == 2 && (CompararPrefijoConClaveDeLaGlobal(tcCodigo, "wc_cliente", 2) || CompararPrefijoConClaveDeLaGlobal(tcCodigo, "wc_deudor", 2)))
                {
                    return true;
                }
            }

            return false;
        }

        private static bool CompararPrefijoConClaveDeLaGlobal(string tcCandidato, string tcNombreVariable, int tnDigitos = 3)
        {
            bool result = false;
            string text = Convert.ToString(EW_GLOBAL._GetVariable(tcNombreVariable.TrimEnd(), ""));
            MessageBox.Show("AT:\n\nprivate static bool CompararPrefijoConClaveDeLaGlobal(string tcCandidato, string tcNombreVariable, int tnDigitos = 3)");
            MessageBox.Show(
                "tcCandidato: " + tcCandidato
                + "\n" +
                "tcNombreVariable: " + tcNombreVariable
                + "\n" +
                "tnDigitos: " + tnDigitos
            );

            MessageBox.Show(text);
            //if(text.Length >= tnDigitos && tcCandidato.StartsWith(text.Substring(0, tnDigitos)))
            //if(text.Length >= tnDigitos)
            if(tcCandidato.Length >= tnDigitos)
            {
                result = true;
            }

            return result;
        }
    }
}
