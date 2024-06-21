using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using System.Reflection;

//Sage 50
using sage.ew.db;
using sage.ew.empresa;

namespace ExampleSatelite.Sage50.Negocio
{
    public class Company : BaseMaster
    {
        #region propiedades

        #endregion propiedades

        #region constructor
        public Company()
        {
            psDb = "gestion";
            psTable = "empresa";
        }
        #endregion constructor

  
    }
}
