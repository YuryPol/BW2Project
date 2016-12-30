package invmanage2;

public class GenerateInventory 
{	
	public static void main(String[] args) {
		int inventorysets_count = Integer.parseInt(args[0]);
		int segments_count = Integer.parseInt(args[1]);
		int max_segment_count = Integer.parseInt(args[2]);
		
		InventoryGenerator.doIt(inventorysets_count, segments_count, max_segment_count);
	}

}
