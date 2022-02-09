using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Tooling.Connector;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Metadata;
using UDS_Test_Stage_2.Enums;

namespace UDS_Test_Stage_2
{
    class Program
    {
        static Random rand = new Random();

        static List<Entity> CarClassList;
        static CrmServiceClient service;
        static List<OptionMetadata> TransferLocation;
        static List<Entity> CustomerList;

        static List<Entity> GetAllByQuery(QueryExpression query)
        {
            var result = new List<Entity>();
            EntityCollection resCol;

            if(query.PageInfo == null)
            {
                query.PageInfo = new PagingInfo
                {
                    Count = 5000,
                    PageNumber = 1,
                    PagingCookie = string.Empty
                };
            }

            do
            {
                resCol = service.RetrieveMultiple(query);
                if (resCol.Entities.Count > 0)
                {
                    result.AddRange(resCol.Entities.ToList());
                }
                if (resCol.MoreRecords)
                {
                    query.PageInfo.PageNumber += 1;
                    query.PageInfo.PagingCookie = resCol.PagingCookie;
                }
            } while (resCol.MoreRecords);

            return result;
        }

        static void SetupCarClassList()
        {
            //  ** Create  Car class list
            QueryExpression query = new QueryExpression()
            {
                Distinct = false,
                EntityName = "new_carclass",
                ColumnSet = new ColumnSet("new_carclassid", "new_classcode", "statecode", "new_price"),
                Criteria =
                {
                    Filters =
                    {
                        new FilterExpression
                        {
                            Conditions =
                            {
                                new ConditionExpression("statecode", ConditionOperator.Equal, 0),  // Only active state
                            },
                        }
                    }
                }
            };
            CarClassList = ((EntityCollection)service.RetrieveMultiple(query)).Entities.ToList();
        }

        static void SetupCustomerList()
        {
            // ** Create Customer List 
            QueryExpression query = new QueryExpression()
            {
                Distinct = false,
                EntityName = "contact",
                ColumnSet = new ColumnSet("contactid", "statecode"),
                Criteria =
                {
                    Filters =
                    {
                        new FilterExpression
                        {
                            Conditions =
                            {
                                new ConditionExpression("statecode", ConditionOperator.Equal, 0),  // Only active state
                            },
                        }
                    }
                }
            };
            CustomerList = GetAllByQuery(query);
        }
        static void SetupTransferLocationList()
        {
            // Create transfer location list
            TransferLocation = service.GetGlobalOptionSetMetadata("new_transferlocation").Options.ToList();
        }
        static void Setup()
        {
            SetupCarClassList();
            SetupCustomerList();
            SetupTransferLocationList();
        }

        static bool PrepareService()
        {
            string connectionString = @"
                AuthType=OAuth;
                Username=;
                Password=;
                Url=https://udstest1.crm4.dynamics.com/;
                AppId=51f81489-12ee-4a9e-aaae-a2591f45987d;
                RedirectUri=app://58145B91-0C36-4500-8554-080854F2AC97;
                LoginPrompt=Never;
                ";

            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            service = new CrmServiceClient(connectionString);

            if (service.IsReady)
            {
                Console.WriteLine("CRM ready");
                return true;
            }
            else
            {
                Console.WriteLine(service.LastCrmError);
                Console.WriteLine("Press any key to exit");
                Console.ReadKey();
                return false;
            }
        }

        static EntityReference CreateReport(TransferType tType, DateTime dat, Guid carId)
        {
            Entity carTransferReport = new Entity("new_cartransferreport");

            carTransferReport["new_date"] = dat;
            carTransferReport["new_transfertype"] = new OptionSetValue((int)tType);
            carTransferReport["new_carid"] = new EntityReference("new_car", carId);
            carTransferReport["new_description"] = (tType == TransferType.Pickup ? "Pickup ": "Return")+" "+dat.ToShortDateString();

            int prob = rand.Next(1, 20);
            if (prob == 1) // 5%
            {
                carTransferReport["new_damages"] = true; // Yes
                carTransferReport["new_damagesdescription"] = "damage";
            }
            else
            {
                carTransferReport["new_damages"] = false; //No
            }

            Guid reportId = service.Create(carTransferReport);
            return new EntityReference("new_cartransferreport", reportId);
        }

        static Entity GetRandomCarClass()
        {
            return CarClassList[rand.Next(CarClassList.Count)];
        }
        static Entity GetRandomCarRespClass(string carClassId)
        {
            //  ** search cars with respect to car class
            QueryExpression query = new QueryExpression()
            {
                Distinct = false,
                EntityName = "new_car",
                ColumnSet = new ColumnSet("new_carclassid", "new_name", "statecode"),
                Criteria =
                {
                    Filters =
                    {
                        new FilterExpression
                        {
                            FilterOperator = LogicalOperator.And,
                            Conditions =
                            {
                                new ConditionExpression("new_carclassid", ConditionOperator.Equal, carClassId),
                                new ConditionExpression("statecode", ConditionOperator.Equal, 0)  // Only active state
                            },
                        },
                    }
                }
            };
            List<Entity> selectedCars = GetAllByQuery(query);
            return selectedCars[rand.Next(selectedCars.Count)];
        }

        static bool CheckCarRental(Guid carId, DateTime dateOfPickup, DateTime dateOfHandover)
        {
            using (OrganizationServiceContext context = new OrganizationServiceContext(service))
            {
                var recs = from rents in context.CreateQuery("new_rent")
                           where (rents.GetAttributeValue<OptionSetValue>("statuscode").Value == (int)RentStatusReason.Canceled)
                           && (
                                (    // this car is fully leased at the moment
                                     (rents.GetAttributeValue<DateTime>("new_reservedpickup") <= dateOfPickup)
                                     && (rents.GetAttributeValue<DateTime>("new_reservedhandover") >= dateOfHandover)
                                )
                                || ( // rental range partially overlaps
                                     (rents.GetAttributeValue<DateTime>("new_reservedpickup") >= dateOfPickup)
                                     && (rents.GetAttributeValue<DateTime>("new_reservedpickup") <= dateOfHandover)
                                     && (rents.GetAttributeValue<DateTime>("new_reservedhandover") >= dateOfHandover)
                                 )
                                || ( // rental range partially overlaps
                                     (rents.GetAttributeValue<DateTime>("new_reservedpickup") <= dateOfPickup)
                                     && (rents.GetAttributeValue<DateTime>("new_reservedhandover") >= dateOfPickup)
                                     && (rents.GetAttributeValue<DateTime>("new_reservedhandover") <= dateOfHandover)
                              )
                            )
                            && (rents.GetAttributeValue<EntityReference>("new_carid").Id == carId)
                           select rents;

                if (recs.ToList().Count() != 0)
                {  // this car is currently rented, we are looking for another
                    return false;
                }
                return true;
            }
        }

        static EntityReference GetRandomCustomer()
        {
            return new EntityReference("contact", new Guid(CustomerList[rand.Next(CustomerList.Count)].Id.ToString()));
        }

        static DateTime GetRandomDay(DateTime startDate, int Duration)
        {
            return startDate.AddDays(rand.Next(Duration+1)); 
        }
                
        static OptionSetValue GetRandomTransferLocation()
        {
            return new OptionSetValue((int)TransferLocation[rand.Next(TransferLocation.Count)].Value); 
        }
        
        static bool GetPaidStatus(OptionSetValue status)
        {
            int prob = rand.Next(1, 10000);
            bool result = false;     // false - No  true - Yes  // another status reason - No
            if (((int)status.Value) == (int)RentStatusReason.Confirmed)
            {
                if (prob <= 9000)
                {
                    result = true;
                }
            }
            else if (((int)status.Value) == (int)RentStatusReason.Renting)
            {
                if (prob <= 9990)
                {
                    result = true;
                }
            }
            else if (((int)status.Value) == (int)RentStatusReason.Returned)
            {
                if (prob <= 9998)
                {
                    result = true;                    
                }
            }
            return result;
        }

        static OptionSetValue GetStatusState(int statuscode)
        {
            if ((statuscode == (int)RentStatusReason.Returned) || (statuscode == (int)RentStatusReason.Canceled))
            {
                return new OptionSetValue(1); // inactive
            }
            else
            {
                return new OptionSetValue(0); // active
            }
        }

        static OptionSetValue GetRandomStatusReason()
        {
            int prob = rand.Next(1,20);
            int StatusCode;
            if (prob==1) // 5% - Created  
            {
                StatusCode = (int)RentStatusReason.Created;
            }
            else if (prob == 2) // 5% - Confirmed
            {
                StatusCode = (int)RentStatusReason.Confirmed;
            }
            else if (prob == 3) // 5% - Renting
            {
                StatusCode = (int)RentStatusReason.Renting;
            }
            else if ((prob >= 4) && (prob <= 18)) // 75% - returned
            {
                StatusCode = (int)RentStatusReason.Returned;
            }
            else // 10% - Canceled
            {
                StatusCode = (int)RentStatusReason.Canceled;
            }

            return new OptionSetValue(StatusCode);
        }

        static void Main(string[] args)
        {

            if(!PrepareService())
            {
                return;
            }

            Setup();

            int TotalSamples = 40000;

            DateTime BaseStartDate = new DateTime(2019, 1, 1);
            DateTime BaseEndDate = new DateTime(2020, 12, 31);
            int MaxDuration = (BaseEndDate - BaseStartDate).Days;

            for (int CurrentSample = 1; CurrentSample <= TotalSamples; CurrentSample++) // loop to create sample data
            {
                Entity rent = new Entity("new_rent");

                string nameOfSample = "        " + CurrentSample.ToString();
                nameOfSample = nameOfSample.Substring(nameOfSample.Length - 7, 7);
                rent["new_name"] = "Sample  -" + nameOfSample;

                bool GoodSampleData = false;
                while (!GoodSampleData) 
                {
                    OptionSetValue rentStatusCode = GetRandomStatusReason();
                    rent["statuscode"] = rentStatusCode;
                    rent["statecode"] = GetStatusState(rentStatusCode.Value);

                    int Duration = rand.Next(1, 30); // Duration of rent 1-30 days
                    
                    DateTime rentDatePickup = GetRandomDay(BaseStartDate, MaxDuration - Duration);
                    rent["new_reservedpickup"] = rentDatePickup;
                    DateTime rentDateHandover = rentDatePickup.AddDays(Duration);
                    rent["new_reservedhandover"] = rentDateHandover;

                    if ((rentStatusCode.Value == (int)RentStatusReason.Renting) || (rentStatusCode.Value == (int)RentStatusReason.Returned))
                    {
                        rent["new_actualpickup"] = rentDatePickup;
                    }
                    if (rentStatusCode.Value == (int)RentStatusReason.Returned)
                    {
                        rent["new_actualreturn"] = rentDateHandover;
                    }

                    Entity randomCarClass = GetRandomCarClass();
                    rent["new_carclassid"] = new EntityReference("new_carclass", randomCarClass.Id);

                    Entity selectedCar = GetRandomCarRespClass(rent.GetAttributeValue<EntityReference>("new_carclassid").Id.ToString());
                    if (!CheckCarRental(selectedCar.Id, rentDatePickup, rentDateHandover))
                    {  // this car is currently rented, we are looking for another
                        continue;
                    }
                    rent["new_carid"] = new EntityReference("new_car", selectedCar.Id);
                    
                    rent["new_pickuplocation"] = GetRandomTransferLocation();
                    rent["new_returnlocation"] = GetRandomTransferLocation();
                    
                    rent["new_price"] = new Money(randomCarClass.GetAttributeValue<Money>("new_price").Value * Duration); // Approx

                    rent["new_customer"] = GetRandomCustomer();

                    rent["new_paid"] = GetPaidStatus(rentStatusCode);

                    if ((rentStatusCode.Value == (int)RentStatusReason.Renting) || (rentStatusCode.Value == (int)RentStatusReason.Returned))
                    {
                        rent["new_pickupreportid"] = CreateReport(TransferType.Pickup, rentDatePickup, selectedCar.Id);
                    }

                    if (rentStatusCode.Value == (int)RentStatusReason.Returned)
                    {
                        rent["new_returnreport"] = CreateReport(TransferType.Return, rentDateHandover, selectedCar.Id);
                    }

                    GoodSampleData = true;
                }
                Guid rentId = service.Create(rent);
                Console.WriteLine(CurrentSample.ToString() + " - " + rentId.ToString());
            }

            Console.WriteLine("Done. Press any key to exit");
            Console.ReadKey();
        }
    }
}
