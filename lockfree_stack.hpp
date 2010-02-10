 

template<typename obj>
class lf_stack{
private:
	class node{
	public:
		obj data;
		node* next;
		node(const obj& d):data(d),next(NULL){ }
	};
	volatile node *head;
public:
	lf_stack(void){
		head=NULL;
	}
	
	obj top(void){
		return head->data;
	}

	int size(void) const {
		volatile node* ptr = head;
		int counter = 0;
		while(ptr!=NULL){
			counter++;
			ptr = ptr->next;
		}
		return counter;
	}
	void clear(void){
		volatile node* ptr = head,*next;
		while(ptr!=NULL){
			next=ptr->next;
			delete ptr;
			ptr = next;
		}
		head = NULL;
		return ;
	}
	bool empty(void) const{
		return head==NULL;
	}
	
	void pop(void) {
		while(1) {
			volatile node* old_head = head;
			if (!old_head){
				head = NULL;
				return;
			}
			if (__sync_bool_compare_and_swap(&head, old_head, old_head->next)) {
				delete old_head;
				return;
			}
		}
	}
	void atomic_pop(obj* target) {
		while(1) {
			volatile node* old_head = head;
			if (!old_head){
				head = NULL;
				return;
			}
			if (__sync_bool_compare_and_swap(&head, old_head, old_head->next)) {
				*target = old_head->data;
				delete old_head;
				return;
			}
		}
	}
	
	void push(const obj& element) {
		volatile node *newnode = new node(element);
		volatile node *old_head;
		while(1) {
			old_head = head;
			newnode->next = const_cast<node*>(old_head);
			
			if (__sync_bool_compare_and_swap(&head, old_head , newnode)) {
				return;
			}
		}
	}
};
