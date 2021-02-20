# -*- coding: utf-8 -*-
# 클래스 설정
import gurobipy as gp
from gurobipy import GRB
import constant_v
import pandas as pd
import gurobipy as gp
from gurobipy import GRB

class Technician():
    def __init__(self, cap, depot, name):
        self.name = name
        self.cap = cap
        self.depot = depot

    def __str__(self):
        return f"Technician: {self.name}\n  Capacity: {self.cap}\n  Depot: {self.depot}"


class Job():
    def __init__(self, name, duration, priority, coveredBy):
        self.name = name
        self.priority = priority
        self.duration = duration
        self.coveredBy = coveredBy  #coveredBy를 새롭게 추가

    def __str__(self):
        about = f"Job: {self.name}\n  Priority: {self.priority}\n  Duration: {self.duration}\n  Covered by: "
        about += ", ".join([t.name for t in self.coveredBy])
        return about


class Customer():
    def __init__(self, name, loc, job, sales, tStart, tEnd, tDue, duration):
        self.name = name
        self.loc = loc
        self.job = job
        self.sales = sales
        self.tStart = tStart
        self.tEnd = tEnd
        self.tDue = tDue
        self.duration = duration

    def __str__(self):
        coveredBy = ", ".join([t.name for t in self.job.coveredBy])
        return f"Customer: {self.name}\n  Location: {self.loc}\n  Job: {self.job.name}\n " \
               f" Priority: {self.job.priority}\n  Duration: {self.job.duration}\n " \
               f" Covered by: {coveredBy}\n  Start time: {self.tStart}\n  End time: {self.tEnd}\n " \
               f" Due time: {self.tDue}"


def run_model(schedule, product, dist, tech, tech_name, point_bag, canCover ):
    tech.loc["names", :] = tech_name

    technicians = []
    for i in iter(tech.T.values):
        thisTech = Technician(*i)
        technicians.append(thisTech)

    product = product.reset_index().rename({'index': 'Name'}, axis=1)

    schedule.loc["coveredby"] = schedule.loc["coveredby"].apply(lambda x: ','.join(x))

    product["coveredBy"] = schedule.loc["coveredby"].values

    jobs = []
    for i in iter(product.values):
        thisJob = Job(*i)
        jobs.append(thisJob)

    locations = point_bag
    dist1 = {(l, l): 0 for l in locations}

    for i, l1 in enumerate(locations):
        for j, l2 in enumerate(locations):
            if i < j:
                dist1[l1, l2] = dist.iloc[i, j]
                dist1[l2, l1] = dist1[l1, l2]

    schedule = schedule.drop(["address", "coveredby"], axis=0)
    customers = []
    for i in iter(schedule.T.values):
        thisCustomer = Customer(*i)
        customers.append(thisCustomer)
    K = [k.name for k in technicians]  # technitian name
    C = [j.name for j in customers]  # Custmer name
    J = [j.loc for j in customers]  # Customer location set

    L = point_bag
    D = list(set([t.depot for t in technicians]))
    cap = {k.name: k.cap for k in technicians}
    loc = {j.name: j.loc for j in customers}
    depot = {k.name: k.depot for k in technicians}
    dur = {j.name: j.duration for j in customers}
    tStart = {j.name: j.tStart for j in customers}
    tEnd = {j.name: j.tEnd for j in customers}
    tDue = {j.name: j.tDue for j in customers}
    priority = {i.name: 1 for i in customers}

    ### Create model
    m = gp.Model("trs0")  # 모델 선언

    ### Decision variables
    # Customer-technician assignment

    # Customer-Technition 조합 (배정)
    x = m.addVars(C, K, vtype=GRB.BINARY, name="x")

    # Technician assignment
    u = m.addVars(K, vtype=GRB.BINARY, name="u")

    y = m.addVars(L, L, K, vtype=GRB.BINARY, name="y")

    # Technician cannot leave or return to a depot that is not its base
    for k in technicians:
        for d in D:
            if k.depot != d:
                for i in L:
                    y[i, d, k.name].ub = 0
                    y[d, i, k.name].ub = 0

    # Start time of service
    # tj≥0 : This variable determines the time to arrive or start the service at location  j∈J .
    t = m.addVars(L, ub=constant_v.workingTime, name="t")

    # Lateness of service
    # zj≥0 : This variable determines the lateness of completing job j∈J.
    z = m.addVars(C, name="z")

    # Artificial variables to correct time window upper and lower limits
    # xaj,xbj≥0 : Correction to earliest and latest time to start the service for job  j∈J .
    xa = m.addVars(C, name="xa")
    xb = m.addVars(C, name="xb")

    # Unfilled jobs
    # gj : This variable is equal 1 if job  j∈J  cannot be filled, and 0 otherwise.
    g = m.addVars(C, vtype=GRB.BINARY, name="g")

    ### Constraints1

    # A technician must be assigned to a job, or a gap is declared (1)

    # ∑x(j,k)+g(j)=1, ∀j∈J
    # k∈K(j)

    # A technician must be assigned to a job, or a gap is declared (1)
    m.addConstrs((gp.quicksum(x[j, k] for k in canCover[j]) + g[j] == 1 for j in C), name="assignToJob")

    # m.addConstrs((gp.quicksum(x[j, k] for k in K) + g[j] == 1 for j in C),
    #              name="assignToJob")

    ### Constraints2

    # At most one technician can be assigned to a job (2)
    # ∑x(j,k)≤1, ∀j∈J
    # k∈K

    m.addConstrs((x.sum(j, '*') <= 1 for j in C), name="assignOne")

    # Technician capacity constraints (3)

    #  ∑p(j)⋅x(j,k)+∑(i∈L)∑(j∈L)τ(i,j)⋅y(i,j,k)≤W(k)⋅u(k), ∀k∈K
    # (j∈J)

    capLHS = {k: gp.quicksum(dur[j] * x[j, k] for j in C) +
                 gp.quicksum(dist1[i, j] * y[i, j, k] for i in L for j in L) for k in K}
    m.addConstrs((capLHS[k] <= cap[k] * u[k] for k in K), name="techCapacity")

    # Technician tour constraints (4 and 5)

    # For each technician and job, we ensure that if the technician is assigned to the job,
    # then the technician must travel to another location (to form a tour).

    m.addConstrs((y.sum('*', loc[j], k) == x[j, k] for k in K for j in C),
                 name="techTour1")
    m.addConstrs((y.sum(loc[j], '*', k) == x[j, k] for k in K for j in C),
                 name="techTour2")

    # constraints 6, 7

    # Same depot constraints (6 and 7)
    # Same depot: For each technician and depot, we ensure that a technician,
    # if assigned to any job, must depart from and return to the service center (depot)
    # where the technician is based.

    m.addConstrs((gp.quicksum(y[j, depot[k], k] for j in J) == u[k] for k in K),
                 name="sameDepot1")
    m.addConstrs((gp.quicksum(y[depot[k], j, k] for j in J) == u[k] for k in K),
                 name="sameDepot2")
    # Temporal constraints (8) for customer locations
    # Temporal relationship: For each location and job,
    # we ensure the temporal relationship between two consecutive jobs served by the same technician.
    # That is, if a technician  k  travels from job  i  to job  j ,
    # then the start of the service time at job  j  must be no less than the completion time of job  i
    # plus the travel time from job  i  to job  j .

    M = {(i, j): constant_v.w_start_time + dur[i] + dist1[loc[i], loc[j]] for i in C for j in C}
    m.addConstrs((t[loc[j]] >= t[loc[i]] + dur[i] + dist1[loc[i], loc[j]]
                  - M[i, j] * (1 - gp.quicksum(y[loc[i], loc[j], k] for k in K))
                  for i in C for j in C), name="tempoCustomer")

    # # Temporal constraints (8) for depot locations
    M = {(i, j): constant_v.w_start_time + dist1[i, loc[j]] for i in D for j in C}
    m.addConstrs((t[loc[j]] >= t[i] + dist1[i, loc[j]] \
                  - M[i, j] * (1 - y.sum(i, loc[j], '*')) for i in D for j in C),
                 name="tempoDepot")
    # Time window: For each job  j∈J  ensure that the time window for the job is satisfied.
    # t : 실제 서비스 시작타임

    # Time window constraints (9 and 10)
    m.addConstrs((t[loc[j]] + xa[j] >= tStart[j] for j in C), name="timeWinA")
    m.addConstrs((t[loc[j]] - xb[j] <= tEnd[j] for j in C), name="timeWinB")

    # Note: To discourage that the time window of a job is violated,
    # we associate the penalty of ( 0.01⋅πj⋅M ) to the correction variables  xaj,xbj .

    # Lateness constraint (11)
    # Lateness constraint: For each job  j∈J  calculate the lateness of the job.
    # Note that since the lateness decision variable  zj  is non-negative, there is no benefit to complete
    # a job before its due date; on the other hand, since the objective function minimizes
    # the total weighted lateness, Constraint (11) should always be binding.

    # Lateness of service
    # zj≥0 : This variable determines the lateness of completing job j∈J.
    # z = m.addVars(C, name="z")

    m.addConstrs((z[j] >= t[loc[j]] + dur[j] - tDue[j] for j in C), name="lateness")
    M = 8100  # 패널티 상수 (Big M)

    ### Objective function
    # Minimize lateness: The Objective function is to minimize the total weighted lateness of all the jobs.

    # penalty wegiht
    pw = 0.01

    m.setObjective(
        z.prod(priority) + gp.quicksum(pw * M * priority[j] * (xa[j] + xb[j])
                                       for j in C) +
        gp.quicksum(M * priority[j] * g[j] for j in C), GRB.MINIMIZE)

    m.write("TRS0.lp")
    m.optimize()

    jobStrList = []
    notAssined = []
    startCorrectedAssined = []
    endCorrectedAssined = []
    lateAssined = []

    for j in customers:
        if g[j.name].X > 0.5:
            jobStr = "Nobody assigned to {} ({}) in {}".format(j.name, j.job, j.loc)
            notAssined.append(notAssined)
        else:
            for k in K:
                if x[j.name, k].X > 0.5:
                    jobStr = "{} assigned to {} ({}) in {}. Start at t={:.2f}.".format(
                        k, j.name, j.name, j.loc, t[j.loc].X)
                    if z[j.name].X > 1e-6:
                        jobStr += " {:.2f} minutes late.".format(z[j.name].X)
                        lateAssined.append(jobStr)
                    if xa[j.name].X > 1e-6:
                        jobStr += " Start time corrected by {:.2f} minutes.".format(
                            xa[j.name].X)
                        startCorrectedAssined.append(jobStr)
                    if xb[j.name].X > 1e-6:
                        jobStr += " End time corrected by {:.2f} minutes.".format(
                            xb[j.name].X)
                        endCorrectedAssined.append(jobStr)
        print(jobStr)
        jobStrList.append(jobStr)

    # Technicians
    # print("")
    routeDic = []
    notUsedT = []
    routeList = pd.DataFrame(columns=['name', 'location', 'start', 'end'])
    emptyTime = []
    for k in technicians:
        if u[k.name].X > 0.5:
            cur = k.depot
            route = k.depot
            while True:
                for j in customers:
                    if y[cur, j.loc, k.name].X > 0.5:
                        route += " -> {} (dist1={}, t={:.2f}, proc={})".format(
                            j.loc, dist1[cur, j.loc], t[j.loc].X, j.duration)
                        routeList = routeList.append(
                            {'name': k.name, 'location': j.loc,
                             'start': round(t[j.loc].X), 'end': round(t[j.loc].X + j.duration)},
                            ignore_index=True)
                        cur = j.loc
                for i in D:
                    if y[cur, i, k.name].X > 0.5:
                        route += " -> {} (dist1={})".format(i, dist1[cur, i])
                        cur = i
                        break
                if cur == k.depot:
                    break
            # print("{}'s route: {}".format(k.name, route))
            routeDic.append([k.name, route])
        else:
            print("{} is not used".format(k.name))
            notUsedT.append(k.name)

    for k in K:
        used = float(capLHS[k].getValue())
        total = float(cap[k])
        util = used / float(cap[k]) if float(cap[k]) > 0 else 0
        print("{}'s utilization is {:.2%} ({:.2f}/{:.2f})".format(k, util, used, float(cap[k])))
    totUsed = sum(capLHS[k].getValue() for k in K)
    totCap = sum(float(cap[k]) for k in K)
    totUtil = totUsed / totCap if totCap > 0 else 0
    print("Total technician utilization is {:.2%} ({:.2f}/{:.2f})".format(totUtil, totUsed, totCap))
    return totUtil, jobStrList, routeDic, totCap, totUsed, notUsedT, notAssined, startCorrectedAssined, \
           endCorrectedAssined, lateAssined, routeList