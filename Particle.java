package apache_beam_pso;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

//for each particle i = 1, ..., S do
//        Initialize the particle's position with a uniformly distributed random vector: xi ~ U(blo, bup)
//        Initialize the particle's best known position to its initial position: pi ← xi
//        if f(pi) < f(g) then
//        update the swarm's best known position: g ← pi
//        Initialize the particle's velocity: vi ~ U(-|bup-blo|, |bup-blo|)
//        while a termination criterion is not met do:
//        for each particle i = 1, ..., S do
//        for each dimension d = 1, ..., n do
//        Pick random numbers: rp, rg ~ U(0,1)
//        Update the particle's velocity: vi,d ← w vi,d + φp rp (pi,d-xi,d) + φg rg (gd-xi,d)
//        Update the particle's position: xi ← xi + vi
//        if f(xi) < f(pi) then
//        Update the particle's best known position: pi ← xi
//        if f(pi) < f(g) then
//        Update the swarm's best known position: g ← pi
public class particle {
    //private variable
    public int id;
    public int[] neighborhood;
    public double[] position;
    public double fitness;
    public double[] p_best;
    public double p_fitness;
    //parallel pso,multiple threading safe
    public static volatile double[] swarm_best = {Double.MAX_VALUE, Double.MAX_VALUE};
    public static volatile double swarm_best_fitness = Double.MAX_VALUE;
    public double[] velocity;
    public double w;
    public double rp;
    public double rg;
    public double[] b_lower;
    public double[] b_upper;
    public int swarm_size;
    public static Random r = new Random();
    public static volatile double particle_best_fitness;
    public static volatile double[] particle_best;


    //use pso formula update position
    public static particle update_position(particle p) {
        for (int i = 0; i < p.position.length; i++) {
            //                Update the particle's position: xi ← xi + vi
            //can be update by lr
            p.position[i] = p.position[i] + p.velocity[i];
        }
        return p;
    }

    //write the result in the file
    public static void write_file(String path, double fitness) throws IOException {
        FileWriter writer = new FileWriter(path, true);
        writer.write(fitness + System.lineSeparator());
        writer.close();
    }

    public static particle update_velocity(particle p) {
        //        Update the particle's velocity: vi,d ← w vi,d + φp rp (pi,d-xi,d) + φg rg (gd-xi,d)
//        vt+1 = χ [vt + φ1 U() ⊗ (p − xt) + φ2 U() ⊗ (g − xt)]
        double k = 1;
        double phi_1 = 2.05;
        double phi_2 = 2.05;
        double phi = phi_1 + phi_2;
        double chi = 2 * k / Math.abs(2 - phi - Math.sqrt(phi * phi - 4 * phi));
        p.w = chi;
        p.rp = r.nextDouble();
        p.rg = r.nextDouble();
        for (int i = 0; i < p.velocity.length; i++) {
//            p.velocity[i] = chi * (p.velocity[i] + phi_1 * p.rp * (p.p_best[i] - p.position[i]) + phi_2 * p.rg * (swarm_best[i] - p.position[i]));
            //use particle's best position
            p.velocity[i] = chi * (p.velocity[i] + phi_1 * p.rp * (p.p_best[i] - p.position[i]) + phi_2 * p.rg * (p.particle_best[i] - p.position[i]));
        }

        //see the formula update data
//        p.particle_best = swarm_best;
//        p.particle_best_fitness = fitness_function(swarm_best);
        //the rp represent the parameter of p_best
        p.rp = phi_1 * p.rp * chi;
        p.rg = phi_2 * p.rg * chi;
        return p;
    }

    public static particle limit_velocity_position(particle p) {
        for (int i = 0; i < p.velocity.length; i++) {
            double velocity_bound_upper = Math.abs(p.b_upper[i] - p.b_lower[i]);
            double velocity_bound_lower = -velocity_bound_upper;

            //if velocity beyond the velocity limit,reset it in range
            if (p.velocity[i] > velocity_bound_upper || p.velocity[i] < velocity_bound_lower) {
                p.velocity[i] = r.nextDouble() * (2 * velocity_bound_upper) + velocity_bound_lower;
            }

            //if position beyond the position limit,reset it in range
            if (p.position[i] > p.b_upper[i] || p.position[i] < p.b_lower[i]) {
                p.position[i] = r.nextDouble() * (p.b_upper[i] - p.b_lower[i]) + p.b_lower[i];
            }
        }
        return p;
    }

    public static particle particle_init(particle p) {
        //array dimension
        p.w = r.nextDouble();
        p.rp = r.nextDouble();
        p.rg = r.nextDouble();

        double dimension = p.position.length;
        //        Initialize the particle's velocity: vi ~ U(-|bup-blo|, |bup-blo|)
        for (int i = 0; i < dimension; i++) {
//            p.velocity[i] = r.nextDouble() * (2 * Math.abs(p.b_upper[i] - p.b_lower[i])) - Math.abs(p.b_upper[i] - p.b_lower[i]);
            //initialize half of the swarm's velocity as the b_lower,half is b_upper
            if (r.nextDouble() < 0.5) {
                p.velocity[i] = p.b_lower[i];
            } else {
                p.velocity[i] = p.b_upper[i];
            }
        }
        //        Initialize the particle's position with a uniformly distributed random vector: xi ~ U(blo, bup)
        for (int i = 0; i < dimension; i++) {
//            p.position[i] = r.nextDouble() * (p.b_upper[i] - p.b_lower[i]) + p.b_lower[i];
            if (r.nextDouble() < 0.5) {
                p.position[i] = p.b_lower[i];
            } else {
                p.position[i] = p.b_upper[i];
            }
        }
        //        Initialize the particle's best known position to its initial position: pi ← xi
        p.p_best = p.position.clone();
//        if f(pi) < f(g) then
//        update the swarm's best known position: g ← pi
        p.fitness = fitness_function(p.position);
        p.p_fitness = p.fitness;

        swarm_best_fitness = fitness_function(swarm_best);
        // update static swarm_best_fitness
        if (compare_fitness(swarm_best_fitness, p.p_fitness)) {
            //update swarm best_fitness
            swarm_best = p.p_best.clone();
            swarm_best_fitness = p.p_fitness;
        }
        //update particle best_fitness
        p.particle_best = swarm_best;
        p.particle_best_fitness = swarm_best_fitness;
        return p;
    }

    public static double fitness_function(double[] position) {
        double result = 0;
        for (double v : position) {
            result += v * v;
        }
        return result;
    }

    //compare two array fitness
    public static boolean compare_fitness(double a_fitness, double b_fitness) {
        return a_fitness > b_fitness;
    }

    //double array to string
    public String array_to_string(double[] array) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < array.length; i++) {
            result.append(array[i]);
            if (i != (array.length - 1)) {
                result.append(",");
            }
        }
        return result.toString();
    }

    //int array to string
    public String array_to_string(int[] array) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < array.length; i++) {
            result.append(array[i]);
            if (i != (array.length - 1)) {
                result.append(",");
            }
        }
        return result.toString();
    }

    public static particle string_to_particle(String line) {
        particle p = new particle();
        String[] result = line.split(";");
        p.id = Integer.parseInt(result[0]);

        //in order to avoid swarm_g_best flush by other value,calculate the fitness value than Assignment
        double temp = Double.parseDouble(result[2]);
        if (compare_fitness(swarm_best_fitness, temp)) {
            swarm_best = string_to_array(result[1]);
            swarm_best_fitness = temp;
        }

        p.position = string_to_array(result[3]);
        p.fitness = Double.parseDouble((result[4]));

        p.p_best = string_to_array(result[5]);
        p.p_fitness = Double.parseDouble((result[6]));

        p.velocity = string_to_array(result[7]);

        p.w = Double.parseDouble(result[8]);
        p.rp = Double.parseDouble(result[9]);
        p.rg = Double.parseDouble(result[10]);
        p.b_lower = string_to_array(result[11]);
        p.b_upper = string_to_array(result[12]);
        p.swarm_size = Integer.parseInt(result[13]);
        p.neighborhood = string_to_int_array(result[14]);
        p.particle_best = string_to_array(result[15]);
        p.particle_best_fitness = Double.parseDouble(result[16]);
        return p;
    }

    //double string to array
    public static double[] string_to_array(String s) {
        String[] s_array = s.split(",");
        double[] result = new double[s_array.length];
        for (int i = 0; i < s_array.length; i++) {
            result[i] = Double.parseDouble(s_array[i]);
        }
        return result;
    }

    //double string to array
    public static int[] string_to_int_array(String s) {
        String[] s_array = s.split(",");
        int[] result = new int[s_array.length];
        for (int i = 0; i < s_array.length; i++) {
            result[i] = Integer.parseInt((s_array[i]));
        }
        return result;
    }

    @Override
    public String toString() {
        return
                id +
                        ";" + array_to_string(swarm_best) +
                        ";" + swarm_best_fitness +
                        ";" + array_to_string(position) +
                        ";" + fitness +
                        ";" + array_to_string(p_best) +
                        ";" + p_fitness +
                        ";" + array_to_string(velocity) +
                        ";" + w +
                        ";" + rp +
                        ";" + rg +
                        ";" + array_to_string(b_lower) +
                        ";" + array_to_string(b_upper) +
                        ";" + swarm_size +
                        ";" + array_to_string(neighborhood) +
                        ";" + array_to_string(particle_best) +
                        ";" + particle_best_fitness;
    }
}
